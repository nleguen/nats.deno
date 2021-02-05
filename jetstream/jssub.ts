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

import { NatsError } from "../nats-base-client/error.ts";
import { JsMsg, NextRequest } from "./jstypes.ts";
import {
  Msg,
  NatsConnection,
  Subscription,
  SubscriptionOptions,
} from "../nats-base-client/types.ts";
import { SubscriptionImpl } from "../nats-base-client/subscription.ts";
import { NatsConnectionImpl } from "../nats-base-client/nats.ts";
import { JsMsgImpl } from "./jsmsg.ts";
import { Codec, JSONCodec } from "../nats-base-client/mod.ts";

export type JsCallback = (err: NatsError | null, msg: JsMsg) => void;

export interface SubConsumerInfo {
  stream?: string;
  consumer?: string;
  deliver?: string;
  durable?: boolean;
  pull?: number;
  attached?: boolean;
}

export interface PullSubscription<T> extends Subscription<T> {
  pull(): void;
}

export class JsSubscriptionImpl<T> extends SubscriptionImpl<T>
  implements PullSubscription<T> {
  nc: NatsConnection;
  info: SubConsumerInfo;
  prefix: string;
  jc?: Codec<unknown>;

  constructor(
    nc: NatsConnectionImpl,
    subject: string,
    prefix: string,
    opts: SubscriptionOptions,
    manualAcks: boolean,
  ) {
    super(nc.protocol, subject, opts);
    this.nc = nc;
    this.prefix = prefix;
    this.info = {} as SubConsumerInfo; //setup later

    if (!manualAcks) {
      this.postYield = (m: T): void => {
        const um = m as unknown;
        const jm = um as JsMsg;
        jm.ack();
      };
    }
  }

  callback(err: NatsError | null, msg: Msg): void {
    this.cancelTimeout();
    const u = new JsMsgImpl(msg, this.subject) as unknown;
    const jm = u as T;
    err ? this.stop(err) : this.push(jm);
  }

  pull(): void {
    if (!this.info.deliver || this.info.pull === 0) {
      throw new Error("not a pull subscription");
    }
    if (!this.jc) {
      this.jc = JSONCodec();
    }
    const d = { batch: this.info.pull } as NextRequest;
    this.nc.publish(
      `${this.prefix}.CONSUMER.NEXT.${this.info.stream}.${this.info.consumer}`,
      this.jc.encode(d),
      { reply: this.subject },
    );
  }
}
