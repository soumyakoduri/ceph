// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/* The ITE periodicity detector: the FFT math against the naive sums, the
 * verdicts on known patterns, and the weekly worker over in-memory
 * contracts. */

#include "rgw_ite_fft_detector.h"

#include <chrono>
#include <cmath>
#include <complex>
#include <random>

#include <gtest/gtest.h>

#include "common/ceph_context.h"
#include "global/global_context.h"
#include "rgw_ite.h"
#include "rgw_ite_fft.h"
#include "ite_fakes.h"

using namespace rgw::ite;
using namespace rgw::ite::test;

namespace {

constexpr auto DAY = std::chrono::hours(24);

/* a series of n windows with one access in each of the given windows */
std::vector<double> spikes(size_t n, std::initializer_list<size_t> at)
{
  std::vector<double> s(n, 0.0);
  for (auto i : at) {
    s[i] += 1;
  }
  return s;
}

std::vector<double> every(size_t n, size_t period, size_t phase)
{
  std::vector<double> s(n, 0.0);
  for (size_t i = phase; i < n; i += period) {
    s[i] = 1;
  }
  return s;
}

} // anonymous namespace

TEST(ITEFFT, MatchesTheNaiveTransform)
{
  std::mt19937 rng(7);
  std::uniform_real_distribution<double> u(-1, 1);
  const size_t n = 64;
  std::vector<std::complex<double>> a(n);
  for (auto& c : a) {
    c = {u(rng), u(rng)};
  }
  auto f = a;
  fft(f);
  for (size_t k = 0; k < n; ++k) {
    std::complex<double> want = 0;
    for (size_t i = 0; i < n; ++i) {
      want += a[i] * std::polar(1.0, -2 * std::numbers::pi * i * k / n);
    }
    EXPECT_NEAR(want.real(), f[k].real(), 1e-9) << k;
    EXPECT_NEAR(want.imag(), f[k].imag(), 1e-9) << k;
  }
  fft(f, true);
  for (size_t i = 0; i < n; ++i) {
    EXPECT_NEAR(a[i].real(), f[i].real() / n, 1e-12);
    EXPECT_NEAR(a[i].imag(), f[i].imag() / n, 1e-12);
  }
}

TEST(ITEFFT, AutocorrelationIsLinear)
{
  std::mt19937 rng(11);
  std::uniform_real_distribution<double> u(-1, 1);
  std::vector<double> x(90);
  for (auto& v : x) {
    v = u(rng);
  }
  const auto r = autocorrelation(x);
  ASSERT_EQ(x.size(), r.size());
  for (size_t k = 0; k < x.size(); ++k) {
    double want = 0;
    for (size_t i = 0; i + k < x.size(); ++i) {
      want += x[i] * x[i + k];
    }
    EXPECT_NEAR(want, r[k], 1e-9) << k; // no wrap-around
  }
  EXPECT_TRUE(autocorrelation({}).empty());
}

TEST(ITEPeriodicity, Weekly)
{
  const auto p = detect_period(every(90, 7, 3), 3, 0.5);
  EXPECT_TRUE(p.periodic);
  EXPECT_EQ(7u, p.period); // not a multiple of it
  EXPECT_GT(p.confidence, 0.8);
}

TEST(ITEPeriodicity, EveryOtherWindow)
{
  const auto p = detect_period(every(90, 2, 1), 3, 0.5);
  EXPECT_TRUE(p.periodic);
  EXPECT_EQ(2u, p.period);
}

TEST(ITEPeriodicity, MonthlyOnceACycle)
{
  // the longest period three cycles of a 90-window span can show
  const auto p = detect_period(spikes(90, {5, 35, 65}), 3, 0.5);
  EXPECT_TRUE(p.periodic);
  EXPECT_EQ(30u, p.period);
}

TEST(ITEPeriodicity, MonthlyWithCalendarJitter)
{
  // 31 days, then 29
  const auto p = detect_period(spikes(90, {5, 36, 65}), 3, 0.5);
  EXPECT_TRUE(p.periodic);
  EXPECT_NEAR(30, p.period, 1);
}

TEST(ITEPeriodicity, DenseBlocks)
{
  // a busy first week of every month
  std::vector<double> s(90, 0.0);
  for (size_t c : {0, 30, 60}) {
    for (size_t d = 0; d < 7; ++d) {
      s[c + d] = 3;
    }
  }
  const auto p = detect_period(s, 3, 0.5);
  EXPECT_TRUE(p.periodic);
  EXPECT_EQ(30u, p.period);
}

TEST(ITEPeriodicity, BurstIsAperiodic)
{
  std::vector<double> s(90, 0.0);
  for (size_t d = 40; d < 50; ++d) {
    s[d] = 1;
  }
  EXPECT_FALSE(detect_period(s, 3, 0.5).periodic);
}

TEST(ITEPeriodicity, ConstantIsAperiodic)
{
  EXPECT_FALSE(detect_period(std::vector<double>(90, 2.0), 3, 0.5).periodic);
}

TEST(ITEPeriodicity, PeriodLongerThanTheSpanAllows)
{
  // every 40 windows: only two full cycles fit in 90
  EXPECT_FALSE(detect_period(spikes(90, {5, 45, 85}), 3, 0.5).periodic);
}

TEST(ITEPeriodicity, TooFewAccessesOrWindows)
{
  const auto p = detect_period(spikes(90, {10, 40}), 3, 0.5);
  EXPECT_FALSE(p.periodic);
  EXPECT_EQ(0.0, p.confidence);
  EXPECT_FALSE(detect_period(spikes(5, {0, 2, 4}), 3, 0.5).periodic);
}

TEST(ITEPeriodicity, ConfidenceThreshold)
{
  const auto s = spikes(90, {5, 35, 65});
  const auto p = detect_period(s, 3, 0.5);
  ASSERT_TRUE(p.periodic);
  const auto strict = detect_period(s, 3, p.confidence + 0.01);
  EXPECT_FALSE(strict.periodic);
  EXPECT_DOUBLE_EQ(p.confidence, strict.confidence);
}

TEST(ITEPeriodicity, RandomAccessIsRarelyPeriodic)
{
  std::mt19937 rng(42);
  std::uniform_int_distribution<size_t> day(0, 89);
  std::uniform_int_distribution<size_t> accesses(6, 12);
  int periodic = 0;
  for (int t = 0; t < 100; ++t) {
    std::vector<double> s(90, 0.0);
    for (size_t i = accesses(rng); i > 0; --i) {
      s[day(rng)] += 1;
    }
    periodic += detect_period(s, 3, 0.5).periodic;
  }
  EXPECT_LE(periodic, 5);
}

namespace {

struct FFTTest : public ::testing::Test {
  DoutPrefix dpp{g_ceph_context, ceph_subsys_rgw, "fft test: "};
  rgw::ite::Context ctx;
  MemAggStore* agg = nullptr;
  MemStateStore* state = nullptr;
  const rgw_bucket b1{"", "periodic", "periodic-id"};
  const rgw_bucket b2{"", "disabled", "disabled-id"};
  TimeScale ts;
  Window today;
  ceph::real_time now; // noon of today, so yesterday has settled
  std::map<std::string, std::string> saved;

  void SetUp() override {
    ctx.cct = g_ceph_context;
    ctx.cookie = "this-gateway";
    auto as = std::make_unique<MemAggStore>(4);
    agg = as.get();
    ctx.agg = std::move(as);
    auto ss = std::make_unique<MemStateStore>();
    state = ss.get();
    ctx.state = std::move(ss);
    ctx.bucket_loader = [this](const DoutPrefixProvider*, const rgw_bucket& b,
                               std::unique_ptr<rgw::sal::Bucket>* out, optional_yield) {
      *out = make_bucket(b, b != b2);
      return 0;
    };
    // three weeks of daily windows: periods up to 7 windows
    set_conf("rgw_ite_fft_span", "21");
    set_conf("rgw_ite_fft_warmup", "0");
    ts = ctx.timescale();
    today = Window::of(ceph::real_clock::now(), Granularity::Day, ts);
    now = today.start(ts) + std::chrono::hours(12);
    enable(b1, now - 30 * DAY);
  }

  void TearDown() override {
    for (const auto& [k, v] : saved) {
      g_conf().set_val_or_die(k, v);
    }
  }

  void set_conf(const std::string& key, const std::string& val) {
    std::string old;
    ASSERT_EQ(0, g_conf().get_val(key, &old));
    saved.try_emplace(key, old);
    g_conf().set_val_or_die(key, val);
  }

  void enable(const rgw_bucket& b, ceph::real_time at) {
    BucketProgress p;
    p.bucket = b;
    p.enabled_at = at;
    agg->dir[b.get_key()] = p;
  }

  Window back(uint64_t days) const { return Window{Granularity::Day, today.index - days}; }

  /* one GET of name in each of the windows that many days back */
  void reads(const std::string& name, uint64_t gen, std::initializer_list<uint64_t> days) {
    for (auto d : days) {
      add_count(*agg, b1, back(d), name, gen, Op::Get, 1);
    }
  }

  const rgw_bucket_ite_state& ite(const std::string& name) {
    auto e = state->find(b1, rgw_obj_key(name));
    EXPECT_TRUE(e);
    return e->ite;
  }

  int run(FFTStats* stats = nullptr, ceph::real_time at = {}) {
    FFTDetector detector(ctx);
    int r = detector.process_bucket(&dpp, b1, at == ceph::real_time() ? now : at, null_yield);
    if (stats) {
      *stats = detector.stats();
    }
    return r;
  }

  BucketProgress progress(const rgw_bucket& b) { return agg->dir[b.get_key()]; }
};

} // anonymous namespace

TEST_F(FFTTest, FindsAWeeklyPatternAndPredictsTheNextAccess)
{
  state->put(b1, "report", 5);
  reads("report", 5, {1, 8, 15});
  FFTStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(1u, st.buckets);
  EXPECT_EQ(1u, st.candidates);
  EXPECT_EQ(1u, st.periodic);
  const auto& s = ite("report");
  EXPECT_TRUE(s.fft_periodic);
  EXPECT_EQ(7u, s.fft_period_days);
  EXPECT_GE(s.fft_confidence, 0.5);
  // a week after the last read, which was yesterday
  const Window next{Granularity::Day, back(1).index + 7};
  EXPECT_EQ(to_epoch_secs(next.start(ts)), s.fft_next_access);
  EXPECT_EQ(now, progress(b1).last_fft);
}

TEST_F(FFTTest, AperiodicVerdictReplacesAStaleOne)
{
  auto& e = state->put(b1, "burst", 5);
  e.ite.fft_periodic = true;
  e.ite.fft_period_days = 7;
  e.ite.fft_next_access = 12345;
  e.ite.fft_confidence = 0.9;
  reads("burst", 5, {1, 2, 3});
  FFTStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(1u, st.aperiodic);
  const auto& s = ite("burst");
  EXPECT_FALSE(s.fft_periodic);
  EXPECT_EQ(0u, s.fft_period_days);
  EXPECT_EQ(0u, s.fft_next_access);
}

TEST_F(FFTTest, OnlyTheCurrentInstanceIsTested)
{
  state->put(b1, "rare", 5);
  state->put(b1, "replaced", 9); // overwritten at generation 9
  reads("rare", 5, {1, 8});         // two windows are not enough to test
  reads("replaced", 5, {1, 8, 15}); // the weekly reads were of the old instance
  FFTStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(0u, st.candidates);
  EXPECT_EQ(2u, st.sparse);
  EXPECT_EQ(1u, st.stale);
  EXPECT_EQ(0, state->sets);
  EXPECT_FALSE(ite("replaced").fft_periodic);
}

TEST_F(FFTTest, OverwriteDuringAnalysisIsRefused)
{
  state->put(b1, "report", 5);
  reads("report", 5, {1, 8, 15});
  state->before_set = [&](const rgw_obj_key& k) { state->overwrite(b1, k.name, 12); };
  FFTStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(1u, st.stale);
  EXPECT_FALSE(ite("report").fft_periodic); // the new instance has no history
}

TEST_F(FFTTest, RunsOncePerPeriod)
{
  state->put(b1, "report", 5);
  reads("report", 5, {1, 8, 15});
  ASSERT_EQ(0, run());
  EXPECT_EQ(now, progress(b1).last_fft);

  FFTStats st;
  ASSERT_EQ(0, run(&st, now + DAY));
  EXPECT_EQ(1u, st.buckets_not_due);
  EXPECT_EQ(0u, st.buckets);

  const auto later = now + ctx.period("rgw_ite_fft_period");
  ASSERT_EQ(0, run(&st, later));
  EXPECT_EQ(1u, st.buckets);
  EXPECT_EQ(later, progress(b1).last_fft);
}

TEST_F(FFTTest, UnchangedVerdictIsNotRewritten)
{
  state->put(b1, "report", 5);
  reads("report", 5, {1, 8, 15});
  ASSERT_EQ(0, run());
  EXPECT_EQ(1, state->sets);

  auto p = progress(b1);
  p.last_fft = ceph::real_time(); // due again, over the same span
  agg->dir[b1.get_key()] = p;
  FFTStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(1u, st.candidates);
  EXPECT_EQ(1u, st.unchanged);
  EXPECT_EQ(1, state->sets);
}

TEST_F(FFTTest, WaitsForWarmUp)
{
  enable(b1, now - 10 * DAY); // ten days of history; the warm-up is one span
  state->put(b1, "report", 5);
  reads("report", 5, {1, 8});
  FFTStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(1u, st.buckets_warming);
  EXPECT_EQ(0u, st.buckets);
  EXPECT_EQ(ceph::real_time(), progress(b1).last_fft);

  set_conf("rgw_ite_fft_warmup", "7");
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(1u, st.buckets);
}

TEST_F(FFTTest, WeekWindows)
{
  set_conf("rgw_ite_window_granularity", "week");
  set_conf("rgw_ite_fft_span", "12");
  const Window week = Window::of(now, Granularity::Week, ts);
  enable(b1, now - 7 * 20 * DAY);
  state->put(b1, "monthly", 5);
  for (uint64_t w : {1, 5, 9}) { // every four weeks
    add_count(*agg, b1, Window{Granularity::Week, week.index - w}, "monthly", 5, Op::Get, 1);
  }
  FFTStats st;
  ASSERT_EQ(0, run(&st, week.start(ts) + 3 * DAY));
  EXPECT_EQ(1u, st.periodic);
  EXPECT_TRUE(ite("monthly").fft_periodic);
  EXPECT_EQ(28u, ite("monthly").fft_period_days);
  const Window next{Granularity::Week, week.index - 1 + 4};
  EXPECT_EQ(to_epoch_secs(next.start(ts)), ite("monthly").fft_next_access);
}

TEST_F(FFTTest, SkipsBusyAndDisabledBuckets)
{
  enable(b2, now - 30 * DAY);
  agg->locks.busy.insert("bucket." + b1.get_key());
  FFTDetector detector(ctx);
  ASSERT_EQ(0, detector.process(&dpp, null_yield));
  EXPECT_EQ(1u, detector.stats().buckets_busy);
  EXPECT_EQ(1u, detector.stats().buckets_skipped);
  EXPECT_EQ(0u, detector.stats().buckets);
  EXPECT_TRUE(agg->locks.held.empty());
}

TEST_F(FFTTest, ShutdownLeavesTheBucketDue)
{
  state->put(b1, "report", 5);
  reads("report", 5, {1, 8, 15});
  ctx.going_down = true;
  ASSERT_EQ(0, run());
  EXPECT_EQ(ceph::real_time(), progress(b1).last_fft);
  EXPECT_EQ(0, state->sets);
}
