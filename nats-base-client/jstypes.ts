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

import { NatsError } from "./error.ts";
import { BaseMsg, Subscription } from "./types.ts";
import { JsCallback } from "./jssub.ts";

export interface JetStreamClient {
  publish(
    subj: string,
    data: Uint8Array,
    ...options: JetStreamPubOption[]
  ): Promise<PubAck>;
  subscribe(
    subj: string,
    opts: JetStreamSubOptions,
    ...options: JetStreamSubOption[]
  ): Promise<PullSubscription<JsMsg>>;
}

export interface PullSubscription<T> extends Subscription<T> {
  pull(): void;
}

export interface JSM {
  // Create a stream.
  addStream(cfg: StreamConfig): Promise<StreamInfo>;
  // Update a stream
  updateStream(cfg: StreamConfig): Promise<StreamInfo>;
  // Delete a stream
  deleteStream(name: string): Promise<boolean>;
  // Stream information
  streamInfo(name: string): Promise<StreamInfo>;
  // Purge stream messages
  purgeStream(name: string): Promise<void>;
  // newStreamListener is used to return pages of StreamInfo
  // FIXME: this an iterator
  streamLister(): Lister<StreamInfo>;
  // deleteMsg erases a message from a stream
  deleteMsg(name: string, seq: number): Promise<boolean>;

  // Create a consumer
  addConsumer(stream: string, cfg: ConsumerConfig): Promise<ConsumerInfo>;
  // Delete a consumer
  deleteConsumer(stream: string, consumer: string): Promise<boolean>;
  // Consumer information
  consumerInfo(stream: string, name: string): Promise<ConsumerInfo>;
  // newConsumerListener is used to return pages of ConsumerInfo
  consumerLister(stream: string): Lister<ConsumerInfo>;

  // AccountInfo retrieves info about the JetStream usage from an account
  getAccountInfo(): Promise<AccountInfo>;

  streamNameBySubject(subject: string): Promise<string>;
}

export interface JetStreamPubOpts {
  id?: string;
  ttl?: number;
  lid?: string; // expected last message id
  str?: string; // stream name
  seq?: number; // expected last sequence
}

export type JetStreamPubOption = (opts: JetStreamPubOpts) => void;

export function expectLastMsgID(id: string): JetStreamPubOption {
  return (opts: JetStreamPubOpts) => {
    opts.lid = id;
  };
}

export function expectLastSequence(seq: number): JetStreamPubOption {
  return (opts: JetStreamPubOpts) => {
    opts.seq = seq;
  };
}

export function expectStream(stream: string): JetStreamPubOption {
  return (opts: JetStreamPubOpts) => {
    opts.str = stream;
  };
}

export function msgID(id: string): JetStreamPubOption {
  return (opts: JetStreamPubOpts) => {
    opts.id = id;
  };
}

export interface JetStreamSubOptions {
  name?: string;
  stream?: string;
  consumer?: string;
  pull?: number;
  mack?: boolean;
  cfg?: ConsumerConfig;
  queue?: string;
  callback?: JsCallback;
  max?: number;
}

export interface JetStreamSubOpts {
  name: string;
  stream: string;
  consumer: string;
  pull: number;
  mack: boolean;
  cfg: ConsumerConfig;
  queue?: string;
  callback?: (err: (NatsError | null), msg: JsMsg) => void;
  max?: number;
}

export type JetStreamSubOption = (opts: JetStreamSubOpts) => void;

export function validateDurableName(name: string) {
  if (name === "") {
    throw Error("name is required");
  }
  const bad = [".", "*", ">"];
  bad.forEach((v) => {
    if (name.indexOf(v) !== -1) {
      throw Error(`invalid durable name - durable name cannot contain '${v}'`);
    }
  });
}

export function durable(name: string): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    validateDurableName(name);
    opts.cfg.durable_name = name;
  };
}

export function attach(stream: string, consumer: string): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.stream = stream;
    opts.consumer = consumer;
  };
}

export function pull(batchSize: number): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    if (batchSize <= 0) {
      throw new Error("batchsize must be greater than 0");
    }
    opts.pull = batchSize;
  };
}

export function pullDirect(
  stream: string,
  consumer: string,
  batchSize: number,
): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    pull(batchSize)(opts);
    attach(stream, consumer)(opts);
  };
}

export function pushDirect(subject: string): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.cfg.deliver_subject = subject;
  };
}

export function deliverAll(): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.cfg.deliver_policy = DeliverPolicy.All;
  };
}

export function deliverLast(): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.cfg.deliver_policy = DeliverPolicy.Last;
  };
}

export function deliverNew(): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.cfg.deliver_policy = DeliverPolicy.New;
  };
}

export function startSequence(seq: number): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.cfg.deliver_policy = DeliverPolicy.ByStartSequence;
    opts.cfg.opt_start_seq = seq;
  };
}

export function startTime(nanos: number): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.cfg.deliver_policy = DeliverPolicy.ByStartTime;
    opts.cfg.opt_start_seq = nanos;
  };
}

export function manualAck(): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.mack = true;
  };
}

export function ackNone(): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.cfg.ack_policy = AckPolicy.None;
  };
}

export function ackAll(): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.cfg.ack_policy = AckPolicy.All;
  };
}

export function ackExplicit(): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.cfg.ack_policy = AckPolicy.Explicit;
  };
}

export function ns(millis: number) {
  return millis * 1000000;
}

export function ms(ns: number) {
  return ns / 1000000;
}

export interface JetStreamOptions {
  apiPrefix?: string;
  timeout?: number;
  direct?: boolean;
}

export interface StreamConfig {
  name?: string;
  subjects?: string[];
  retention?: RetentionPolicy;
  max_consumers?: number;
  max_msgs?: number;
  max_bytes?: number;
  discard?: DiscardPolicy;
  max_age?: number;
  max_msg_size?: number;
  storage?: StorageType;
  num_replicas?: number;
  no_ack?: boolean;
  duplicate_window?: number;
}

export enum RetentionPolicy {
  Limits = "limits",
  Interest = "interest",
  WorkQueue = "workqueue",
}

export enum DiscardPolicy {
  Old = "old",
  New = "new",
}

export enum StorageType {
  File = "file",
  Memory = "memory",
}

export interface StreamInfo {
  config: StreamConfig;
  created: number; // in ns
  state: StreamState;
  cluster?: ClusterInfo;
}

export interface StreamState {
  messages: number;
  bytes: number;
  first_seq: number;
  first_ts: number;
  last_seq: number;
  last_ts: string;
  consumer_count: number;
}

export interface ClusterInfo {
  name?: string;
  leader?: string;
  replicas?: PeerInfo[];
}

export interface PeerInfo {
  name: string;
  current: boolean;
  active: number; //ns
}

export interface PagedOffset {
  offset: number;
}

export interface ApiPaged {
  total: number;
  offset: number;
  limit: number;
}

export interface ConsumerListResponse extends ApiResponse, ApiPaged {
  consumers: ConsumerInfo[];
}

export interface StreamListResponse extends ApiResponse, ApiPaged {
  streams: StreamInfo[];
}

export interface SuccessResponse extends ApiResponse {
  success: boolean;
}

export interface ConsumerConfig {
  durable_name?: string;
  deliver_subject?: string;
  deliver_policy?: DeliverPolicy;
  opt_start_seq?: number;
  opt_start_time?: number;
  ack_policy?: AckPolicy;
  ack_wait?: number;
  max_deliver?: number;
  filter_subject?: string;
  replay_policy?: ReplayPolicy;
  rate_limit_bps?: number;
  sample_freq?: string;
  max_waiting?: number;
  max_ack_pending?: number;
}

export interface CreateConsumerRequest {
  stream_name: string;
  config: ConsumerConfig;
}

export interface DeleteMsgRequest {
  seq: number;
}

export enum DeliverPolicy {
  All = "all",
  Last = "last",
  New = "new",
  ByStartSequence = "by_start_sequence",
  ByStartTime = "by_start_time",
}

export enum AckPolicy {
  None = "none",
  All = "all",
  Explicit = "explicit",
  NotSet = "",
}

export enum ReplayPolicy {
  Instant = "instant",
  Original = "original",
}

export interface ConsumerInfo {
  stream_name: string;
  name: string;
  created: number;
  config: ConsumerConfig;
  delivered: SequencePair;
  ack_floor: SequencePair;
  num_ack_pending: number;
  num_redelivered: number;
  num_waiting: number;
  num_pending: number;
  cluster?: ClusterInfo;
}

export interface SequencePair {
  consumer_seq: number;
  stream_seq: number;
}

export interface Lister<T> {
  next(): Promise<T[]>;
}

export interface AccountInfo {
  memory: number;
  storage: number;
  streams: number;
  limits: AccountLimits;
}

export interface AccountInfoResponse extends ApiResponse, AccountInfo {}

export interface AccountLimits {
  max_memory: number;
  max_storage: number;
  max_streams: number;
  max_consumers: number;
}

export interface ApiError {
  code: number;
  description: string;
}

export interface ApiResponse {
  type: string;
  error?: ApiError;
}

export enum PubHeaders {
  MsgIdHdr = "Nats-Msg-Id",
  ExpectedStreamHdr = "Nats-Expected-Stream",
  ExpectedLastSeqHdr = "Nats-Expected-Last-Sequence",
  ExpectedLastMsgIdHdr = "Nats-Expected-Last-Msg-Id",
}

export interface PubAck {
  stream: string;
  seq: number;
  duplicate?: boolean;
}

export interface PubAckResponse extends ApiResponse, PubAck {}
export interface StreamInfoResponse extends ApiResponse, StreamInfo {}

export interface JsMsg extends BaseMsg {
  redelivered: boolean;
  info: DeliveryInfo;
  seq: number;

  ack(): void;
  nak(): void;
  working(): void;
  next(): void;
  ignore(): void;
}

export interface DeliveryInfo {
  stream: string;
  consumer: string;
  rcount: number;
  sseq: number;
  dseq: number;
  ts: number;
  pending: number;
}

export interface StreamNames {
  streams: string[];
}

export interface StreamNamesResponse
  extends StreamNames, ApiResponse, ApiPaged {}

export interface StreamNameBySubject {
  subject: string;
}

export interface NextRequest {
  expires: number;
  batch: number;
  no_wait: boolean;
}
