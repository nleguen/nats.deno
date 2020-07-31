/*
 * Copyright 2018-2020 The NATS Authors
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

import { isUint8Array } from "./util.ts";
import {
  Payload,
  ConnectionOptions,
  Msg,
  SubscriptionOptions,
  Status,
} from "./mod.ts";
import {
  ProtocolHandler,
} from "./protocol.ts";
import {
  SubscriptionImpl,
} from "./subscription.ts";
import { ErrorCode, NatsError } from "./error.ts";
import { Nuid } from "./nuid.ts";
import { Subscription, RequestOptions } from "./types.ts";
import { parseOptions } from "./options.ts";
import { QueuedIterator } from "./queued_iterator.ts";
import { MsgHdrs } from "./headers.ts";
import { Request } from "./request.ts";

export const nuid = new Nuid();

export class NatsConnection {
  options: ConnectionOptions;
  protocol!: ProtocolHandler;
  draining: boolean = false;
  listeners: QueuedIterator<Status>[] = [];

  private constructor(opts: ConnectionOptions) {
    this.options = parseOptions(opts);
  }

  public static connect(opts: ConnectionOptions = {}): Promise<NatsConnection> {
    return new Promise<NatsConnection>((resolve, reject) => {
      let nc = new NatsConnection(opts);
      ProtocolHandler.connect(nc.options, nc)
        .then((ph: ProtocolHandler) => {
          nc.protocol = ph;
          (async function () {
            for await (const s of ph.status()) {
              nc.listeners.forEach((l) => {
                l.push(s);
              });
            }
          })();
          resolve(nc);
        })
        .catch((err: Error) => {
          reject(err);
        });
    });
  }

  closed(): Promise<void | Error> {
    return this.protocol.closed;
  }

  async close() {
    await this.protocol.close();
  }

  publish(
    subject: string,
    data: any = undefined,
    options?: { reply?: string; headers?: MsgHdrs },
  ): void {
    subject = subject || "";
    if (subject.length === 0) {
      throw (NatsError.errorForCode(ErrorCode.BAD_SUBJECT));
    }
    // we take string, object to JSON and Uint8Array - if argument is not
    // Uint8Array, then process the payload
    if (!isUint8Array(data)) {
      if (this.options.payload !== Payload.JSON) {
        data = data || "";
      } else {
        data = data === undefined ? null : data;
        data = JSON.stringify(data);
      }
      // here we are a string
      data = new TextEncoder().encode(data);
    }

    this.protocol.publish(subject, data, options);
  }

  subscribe(
    subject: string,
    opts: SubscriptionOptions = {},
  ): Subscription {
    if (this.isClosed()) {
      throw NatsError.errorForCode(ErrorCode.CONNECTION_CLOSED);
    }
    if (this.isDraining()) {
      throw NatsError.errorForCode(ErrorCode.CONNECTION_DRAINING);
    }
    subject = subject || "";
    if (subject.length === 0) {
      throw NatsError.errorForCode(ErrorCode.BAD_SUBJECT);
    }

    const sub = new SubscriptionImpl(this.protocol, subject, opts);
    this.protocol.subscribe(sub);
    return sub;
  }

  request(
    subject: string,
    data: any = undefined,
    opts: RequestOptions = { timeout: 1000 },
  ): Promise<Msg> {
    if (this.isClosed()) {
      return Promise.reject(
        NatsError.errorForCode(ErrorCode.CONNECTION_CLOSED),
      );
    }
    if (this.isDraining()) {
      return Promise.reject(
        NatsError.errorForCode(ErrorCode.CONNECTION_DRAINING),
      );
    }
    subject = subject || "";
    if (subject.length === 0) {
      return Promise.reject(NatsError.errorForCode(ErrorCode.BAD_SUBJECT));
    }
    opts.timeout = opts.timeout || 1000;
    if (opts.timeout < 1) {
      return Promise.reject(new NatsError("timeout", ErrorCode.INVALID_OPTION));
    }

    const r = new Request(this.protocol.muxSubscriptions, opts);
    this.protocol.request(r);

    this.publish(
      subject,
      data,
      {
        reply: `${this.protocol.muxSubscriptions.baseInbox}${r.token}`,
        headers: opts.headers,
      },
    );

    const p = Promise.race([r.timer, r.deferred]);
    p.catch(() => {
      r.cancel();
    });
    return p;
  }

  /***
     * Flushes to the server. Promise resolves when round-trip completes.
     * @returns {Promise<void>}
     */
  flush(): Promise<void> {
    return this.protocol.flush();
  }

  drain(): Promise<void> {
    if (this.isClosed()) {
      return Promise.reject(
        NatsError.errorForCode(ErrorCode.CONNECTION_CLOSED),
      );
    }
    if (this.isDraining()) {
      return Promise.reject(
        NatsError.errorForCode(ErrorCode.CONNECTION_DRAINING),
      );
    }
    this.draining = true;
    return this.protocol.drain();
  }

  isClosed(): boolean {
    return this.protocol.isClosed();
  }

  isDraining(): boolean {
    return this.draining;
  }

  getServer(): string {
    const srv = this.protocol.getServer();
    return srv ? srv.listen : "";
  }

  status(): AsyncIterable<Status> {
    const iter = new QueuedIterator<Status>();
    this.listeners.push(iter);
    return iter;
  }
}
