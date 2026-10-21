// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Intelligent Tiering Engine (ITE): the FFT routines of the periodicity
 * detector. Backend-neutral, header-only math over series of a few
 * hundred points.
 */

#pragma once

#include <bit>
#include <cmath>
#include <complex>
#include <cstddef>
#include <numbers>
#include <utility>
#include <vector>

namespace rgw::ite {

/* in-place iterative radix-2 Cooley-Tukey FFT; a.size() must be a power
 * of two. The inverse is unscaled: fft(fft(a), true) is a.size() * a. */
inline void fft(std::vector<std::complex<double>>& a, bool inverse = false)
{
  const size_t n = a.size();
  // bit-reversal permutation
  for (size_t i = 1, j = 0; i < n; ++i) {
    size_t bit = n >> 1;
    for (; j & bit; bit >>= 1) {
      j ^= bit;
    }
    j ^= bit;
    if (i < j) {
      std::swap(a[i], a[j]);
    }
  }
  for (size_t len = 2; len <= n; len <<= 1) {
    const double angle = (inverse ? 2 : -2) * std::numbers::pi / len;
    for (size_t i = 0; i < n; i += len) {
      for (size_t k = 0; k < len / 2; ++k) {
        const auto w = std::polar(1.0, angle * k);
        const auto u = a[i + k];
        const auto v = a[i + k + len / 2] * w;
        a[i + k] = u + v;
        a[i + k + len / 2] = u - v;
      }
    }
  }
}

/* the linear, not circular, autocorrelation of x at lags 0 to n-1:
 * r[k] = sum over i of x[i] * x[i + k]. Computed from the power spectrum
 * (Wiener-Khinchin), with x zero-padded to a power of two of at least 2n
 * so that the shifted copies never wrap around. */
inline std::vector<double> autocorrelation(const std::vector<double>& x)
{
  const size_t n = x.size();
  if (n == 0) {
    return {};
  }
  const size_t m = std::bit_ceil(2 * n);
  std::vector<std::complex<double>> a(m);
  for (size_t i = 0; i < n; ++i) {
    a[i] = x[i];
  }
  fft(a);
  for (auto& c : a) {
    c = std::norm(c);
  }
  fft(a, true);
  std::vector<double> r(n);
  for (size_t k = 0; k < n; ++k) {
    r[k] = a[k].real() / m;
  }
  return r;
}

} // namespace rgw::ite
