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

import {
  Empty,
  NatsConnection,
  RequestOptions,
} from "../nats-base-client/types.ts";
import { ApiResponse, JetStreamOptions } from "./types.ts";
import { Codec, JSONCodec } from "../nats-base-client/codec.ts";
import { ErrorCode, NatsError } from "../nats-base-client/error.ts";
import { defaultPrefix, defaultTimeout } from "./jetstream.ts";

export class BaseClient {
  nc: NatsConnection;
  opts: JetStreamOptions;
  prefix: string;
  timeout: number;
  jc: Codec<unknown>;

  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    this.nc = nc;
    this.opts = opts ? opts : {} as JetStreamOptions;
    this._parseOpts();
    this.prefix = this.opts.apiPrefix!;
    this.timeout = this.opts.timeout!;
    this.jc = JSONCodec();
  }

  _parseOpts() {
    let prefix = this.opts.apiPrefix || defaultPrefix;
    if (!prefix || prefix.length === 0) {
      throw new Error("invalid empty prefix");
    }
    const c = prefix[prefix.length - 1];
    if (c === ".") {
      prefix = prefix.substr(0, prefix.length - 1);
    }
    this.opts.apiPrefix = prefix;
    this.opts.timeout = this.opts.timeout || defaultTimeout;
  }

  async _request(
    subj: string,
    data: Uint8Array = Empty,
    opts?: RequestOptions,
  ): Promise<unknown> {
    opts = opts || {} as RequestOptions;
    opts.timeout = this.timeout;

    const m = await this.nc.request(
      subj,
      data,
      opts,
    );
    const v = this.jc.decode(m.data);
    const r = v as ApiResponse;

    if (r.error) {
      if (r.error.code === 503) {
        throw NatsError.errorForCode(
          ErrorCode.JETSTREAM_NOT_ENABLED,
          new Error(r.error.description),
        );
      }
      throw new NatsError(r.error.description, `${r.error.code}`);
    }
    return v;
  }
}
