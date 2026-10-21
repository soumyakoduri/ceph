// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/* The output of an ITE admin command, read back as radosgw-admin prints
 * it. A JSONFormattable used as the formatter itself misplaces timestamps:
 * they arrive through dump_stream, which the JSONFormatter finishes only at
 * its next call, after the tree has already moved on to the next section. */

#pragma once

#include <optional>
#include <sstream>
#include <string>

#include "common/ceph_json.h"

namespace rgw::ite::test {

class Output : public ceph::JSONFormatter {
  std::optional<JSONFormattable> tree;

public:
  /* the printed JSON as a tree; nothing is dumped after the first call */
  const JSONFormattable& get() {
    if (!tree) {
      std::stringstream ss;
      flush(ss);
      const std::string s = ss.str();
      tree.emplace();
      JSONParser p;
      if (!s.empty() && p.parse(s.c_str(), s.size())) {
        tree->decode_json(&p);
      }
    }
    return *tree;
  }
  const JSONFormattable& operator[](const std::string& name) { return get()[name]; }
  bool exists(const std::string& name) { return get().exists(name); }
};

} // namespace rgw::ite::test
