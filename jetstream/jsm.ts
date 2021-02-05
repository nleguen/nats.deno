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
  AccountInfo,
  AccountInfoResponse,
  ConsumerConfig,
  ConsumerInfo,
  ConsumerListResponse,
  CreateConsumerRequest,
  DeleteMsgRequest,
  JetStreamManager,
  JetStreamOptions,
  Lister,
  StreamConfig,
  StreamInfo,
  StreamListResponse,
  StreamNameBySubject,
  StreamNames,
  SuccessResponse,
  validateDurableName,
} from "./types.ts";
import { Empty, NatsConnection } from "../nats-base-client/types.ts";
import { ListerFieldFilter, ListerImpl } from "./lister.ts";
import { BaseClient } from "./baseclient.ts";

export class JetStreamManagerImpl extends BaseClient
  implements JetStreamManager {
  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    super(nc, opts);
  }

  async getAccountInfo(): Promise<AccountInfo> {
    const r = await this._request(`${this.prefix}.INFO`);
    return r as AccountInfoResponse;
  }

  async addConsumer(
    stream: string,
    cfg: ConsumerConfig,
  ): Promise<ConsumerInfo> {
    if (!stream) {
      throw new Error("stream name is required");
    }
    const cr = {} as CreateConsumerRequest;
    cr.config = cfg;
    cr.stream_name = stream;

    if (cfg.durable_name) {
      validateDurableName(cfg.durable_name);
    }

    const subj = cfg.durable_name
      ? `${this.prefix}.CONSUMER.DURABLE.CREATE.${stream}.${cfg.durable_name}`
      : `${this.prefix}.CONSUMER.CREATE.${stream}`;
    const r = await this._request(subj, this.jc.encode(cr));
    return r as ConsumerInfo;
  }

  async addStream(cfg = {} as StreamConfig): Promise<StreamInfo> {
    if (!cfg.name) {
      throw Error("stream name is required");
    }
    const r = await this._request(
      `${this.prefix}.STREAM.CREATE.${cfg.name}`,
      this.jc.encode(cfg),
    );
    return r as StreamInfo;
  }

  async consumerInfo(stream: string, name: string): Promise<ConsumerInfo> {
    const r = await this._request(
      `${this.prefix}.CONSUMER.INFO.${stream}.${name}`,
      Empty,
    );
    return r as ConsumerInfo;
  }

  async deleteConsumer(stream: string, durable: string): Promise<boolean> {
    if (!stream || !durable) {
      throw new Error("stream name is required");
    }
    validateDurableName(durable);
    const r = await this._request(
      `${this.prefix}.CONSUMER.DELETE.${stream}.${durable}`,
    );
    const cr = r as SuccessResponse;
    return cr.success;
  }

  async deleteMsg(stream: string, seq: number): Promise<boolean> {
    if (!stream) {
      throw new Error("stream name is required");
    }
    const dr = { seq } as DeleteMsgRequest;
    const r = await this._request(
      `${this.prefix}.STREAM.MSG.DELETE.${stream}`,
      this.jc.encode(dr),
    );
    const cr = r as SuccessResponse;
    return cr.success;
  }

  async deleteStream(stream: string): Promise<boolean> {
    if (!stream) {
      throw new Error("stream name is required");
    }
    const r = await this._request(`${this.prefix}.STREAM.DELETE.${stream}`);
    const cr = r as SuccessResponse;
    return cr.success;
  }

  consumerLister(stream: string): Lister<ConsumerInfo> {
    if (!stream) {
      throw new Error("stream is required");
    }
    const filter: ListerFieldFilter<ConsumerInfo> = (
      v: unknown,
    ): ConsumerInfo[] => {
      const clr = v as ConsumerListResponse;
      return clr.consumers;
    };
    const subj = `${this.prefix}.CONSUMER.LIST.${stream}`;
    return new ListerImpl<ConsumerInfo>(subj, filter, this);
  }

  streamLister(): Lister<StreamInfo> {
    const filter: ListerFieldFilter<StreamInfo> = (
      v: unknown,
    ): StreamInfo[] => {
      const slr = v as StreamListResponse;
      return slr.streams;
    };
    const subj = `${this.prefix}.STREAM.LIST`;
    return new ListerImpl<StreamInfo>(subj, filter, this);
  }

  async purgeStream(name: string): Promise<void> {
    if (!name) {
      throw new Error("stream name is required");
    }
    await this._request(`${this.prefix}.STREAM.PURGE.${name}`);
    return Promise.resolve();
  }

  async streamInfo(name: string): Promise<StreamInfo> {
    if (name === "") {
      throw new Error("stream name is required");
    }
    const r = await this._request(`${this.prefix}.STREAM.INFO.${name}`);
    return r as StreamInfo;
  }

  async streamNameBySubject(subject: string): Promise<string> {
    const q = { subject } as StreamNameBySubject;
    const d = this.jc.encode(q);
    const r = await this._request(`${this.prefix}.STREAM.NAMES`, d);
    const names = r as StreamNames;
    if (names.streams.length != 1) {
      throw new Error("no stream matches subject");
    }
    return names.streams[0];
  }

  async updateStream(cfg = {} as StreamConfig): Promise<StreamInfo> {
    if (!cfg.name) {
      throw new Error("stream name is required");
    }
    const r = await this._request(
      `${this.prefix}.STREAM.UPDATE.${cfg.name}`,
      this.jc.encode(cfg),
    );
    return r as StreamInfo;
  }
}
