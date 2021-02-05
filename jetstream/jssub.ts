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
import {
  ConsumerInfo,
  JetStreamClient, JetStreamSubOptions,
  JetStreamSubOpts,
  JsMsg,
  NextRequest,
} from './types.ts'
import {
  Msg,
  NatsConnection,
  Subscription,
  SubscriptionOptions,
} from "../nats-base-client/types.ts";
import { QueuedIterator } from "../nats-base-client/queued_iterator.ts";
import { JsMsgImpl } from './jsm.ts'
import { JetStreamClientImpl } from './jetstream.ts'

export interface JsSubscriptionOptions {
  queue: string;
  callback?: JsCallback;
}

export type JsCallback = (err: NatsError | null, msg: JsMsg) => void;

export interface JsSubscription extends AsyncIterable<JsMsg> {
  unsubscribe(max?: number): void;
  drain(): Promise<void>;
  isDraining(): boolean;
  isClosed(): boolean;
  callback(err: NatsError | null, msg: Msg): void;
  getSubject(): string;
  getReceived(): number;
  getProcessed(): number;
  getPending(): number;
  getID(): number;
  getMax(): number | undefined;
  pull(): void;
}

export interface SubConsumerInfo {
  stream?: string;
  consumer?: string;
  deliver?: string;
  durable?: boolean
  pull?: number;
  attached?: boolean;
}

export class JsSubscriptionImpl extends QueuedIterator<JsMsg>
  implements JsSubscription {
  js: JetStreamClientImpl;
  sub: Subscription;
  subject: string;
  info: SubConsumerInfo;

  constructor(
    js: JetStreamClientImpl,
    subject: string,
    opts: JetStreamSubOpts,
  ) {
    super();
    this.js = js;
    this.subject = subject;
    this.info = {} as SubConsumerInfo; //setup later

    if (!opts.mack) {
      this.postYield = (m: JsMsg): void => {
        m.ack();
      };
    }
    const o = {} as SubscriptionOptions;
    const cb = opts.callback;
    if (cb) {
      o.callback = (err, msg) => {
        cb(err, new JsMsgImpl(msg, this.subject));
      }
    } else {
      o.callback = (err, msg) => {
        err ? this.stop(err) : this.push(new JsMsgImpl(msg, this.subject));
      };
    }
    o.queue = opts.queue;
    o.max = opts.max;
    this.sub = this.js.nc.subscribe(this.subject, o);
  }

  callback(err: NatsError | null, msg: Msg): void {}

  drain(): Promise<void> {
    return this.sub.drain();
  }

  getID(): number {
    return this.sub.getID();
  }

  getMax(): number | undefined {
    return this.sub.getMax();
  }

  getSubject(): string {
    return this.sub.getSubject();
  }

  isClosed(): boolean {
    return this.sub.isClosed();
  }

  isDraining(): boolean {
    return this.sub.isDraining();
  }

  unsubscribe(max?: number): void {
    return this.sub.unsubscribe(max);
  }

  pull(): void {
    if (!this.info.deliver || this.info.pull === 0) {
      throw new Error("not a pull subscription");
    }
    const d = { batch: this.info.pull } as NextRequest;
    this.js.nc.publish(
      `${this.js.prefix}.CONSUMER.NEXT.${this.info.stream}.${this.info.consumer}`,
      this.js.jc.encode(d),
      { reply: this.subject },
    );
  }
}
