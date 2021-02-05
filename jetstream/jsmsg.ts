/*
 * Copyright 2021 The NATS Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { DeliveryInfo, JsMsg } from "./types.ts";
import { Msg } from "../nats-base-client/types.ts";
import { MsgHdrs } from "../nats-base-client/headers.ts";

const ACK = Uint8Array.of(43, 65, 67, 75);
const NAK = Uint8Array.of(45, 78, 65, 75);
const WPI = Uint8Array.of(43, 87, 80, 73);
const NXT = Uint8Array.of(43, 78, 88, 84);
const TERM = Uint8Array.of(43, 84, 69, 82, 77);

export class JsMsgImpl implements JsMsg {
  msg: Msg;
  srcSubject: string;
  di?: DeliveryInfo;

  constructor(msg: Msg, srcSubject: string) {
    this.msg = msg;
    this.srcSubject = srcSubject;
  }

  get subject(): string {
    return this.msg.subject;
  }

  get sid(): number {
    return this.msg.sid;
  }

  get data(): Uint8Array {
    return this.msg.data;
  }

  get headers(): MsgHdrs | undefined {
    return this.msg.headers;
  }

  get info(): DeliveryInfo {
    // "$JS.ACK.<stream>.<consumer>.<redelivery_count><streamSeq><deliverySequence>.<timestamp>"

    if (!this.di) {
      const tokens = this.reply.split(".");
      const di = {} as DeliveryInfo;
      di.stream = tokens[2];
      di.consumer = tokens[3];
      di.rcount = parseInt(tokens[4], 10);
      di.sseq = parseInt(tokens[5], 10);
      di.dseq = parseInt(tokens[6], 10);
      di.ts = parseInt(tokens[7], 10) / 1000000;
      di.pending = parseInt(tokens[8], 10);
      this.di = di;
    }
    return this.di;
  }

  get redelivered(): boolean {
    return this.info.rcount > 1;
  }

  get reply(): string {
    return this.msg.reply!;
  }

  get seq(): number {
    return this.info.sseq;
  }

  ack() {
    this.msg.respond(ACK);
  }

  nak() {
    this.msg.respond(NAK);
  }

  working() {
    this.msg.respond(WPI);
  }

  next() {
    this.msg.respond(NXT, { reply: this.srcSubject });
  }

  ignore() {
    this.msg.respond(TERM);
  }
}
