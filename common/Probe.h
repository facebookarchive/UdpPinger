/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

namespace facebook {
namespace netnorad {

// the size of the probe body. This should be
// enough to embed the id (signature) and timestamps
const int kNoradProbeDataLen = 32;

//
// Defines the structure of the Netnorad pobe body. The below
// timestamps are all in usecs, and defined with 32-bit resolution
// We assume that this is enough to complete a single probing round
//
struct NoradProbeBody {
  // the signature that the sender puts in, mainly
  // used to identify a valid probe
  uint32_t signature;

  // the timestamp when the probe was sent (usec)
  uint32_t pingerSentTime;

  // the timestamp when probe was received by target
  uint32_t targetRcvdTime;

  // the timestamp when target replied with this probe
  uint32_t targetRespTime;

  // traffic class used by this probe
  uint8_t tclass;
  
  // padding
  char padding[kNoradProbeDataLen - 4 * sizeof(uint32_t) - sizeof(uint8_t)];
};
}
}
