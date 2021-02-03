import type {
  AccountInfo,
  AccountInfoResponse,
  ApiResponse,
  ConsumerConfig,
  ConsumerInfo,
  ConsumerLister,
  DeliveryInfo,
  JetStreamClient,
  JetStreamManager,
  JetStreamOptions,
  JetStreamPubOption,
  JetStreamPubOpts,
  JetStreamSubOpts,
  JsMsg,
  PubAck,
  PubAckResponse,
  StreamConfig,
  StreamInfo,
  StreamLister,
} from "./types.ts";
import {
  Codec,
  Empty,
  ErrorCode,
  headers,
  JSONCodec,
  Msg,
  MsgHdrs,
  NatsConnection,
  NatsError,
  RequestOptions,
  Subscription,
} from "../nats-base-client/mod.ts";
import { JetStreamSubOption, PubHeaders } from "./types.ts";

const defaultPrefix = "$JS.API";
const defaultTimeout = 5000;

export async function JetStream(
  nc: NatsConnection,
  opts: JetStreamOptions = {},
): Promise<JetStreamClient> {
  const ctx = new JetStreamClientImpl(nc, opts);
  if (!ctx.opts.direct) {
    ctx.api = await JSM(nc, opts);
  }
  return ctx;
}

export async function JSM(nc: NatsConnection, opts: JetStreamOptions = {}) {
  const adm = new JetStreamManagerImpl(nc, opts);
  try {
    await adm.getAccountInfo();
  } catch (err) {
    let ne = err as NatsError;
    if (ne.code === ErrorCode.NO_RESPONDERS) {
      ne = NatsError.errorForCode(ErrorCode.JETSTREAM_NOT_ENABLED);
    }
    throw ne;
  }
  return adm;
}

interface reqOpts {
  template: string;
}

class BaseClient {
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

class JetStreamClientImpl extends BaseClient implements JetStreamClient {
  api?: JetStreamManager;
  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    super(nc, opts);
  }

  async publish(
    subj: string,
    data: Uint8Array,
    ...options: JetStreamPubOption[]
  ): Promise<PubAck> {
    const o = {} as JetStreamPubOpts;
    const mh = headers();
    if (options) {
      options.forEach((fn) => {
        fn(o);
      });
      o.ttl = o.ttl || this.timeout;
      if (o.id) {
        mh.set(PubHeaders.MsgIdHdr, o.id);
      }
      if (o.lid) {
        mh.set(PubHeaders.ExpectedLastMsgIdHdr, o.lid);
      }
      if (o.str) {
        mh.set(PubHeaders.ExpectedStreamHdr, o.str);
      }
      if (o.seq && o.seq > 0) {
        mh.set(PubHeaders.ExpectedLastSeqHdr, `${o.seq}`);
      }
    }

    const ro = {} as RequestOptions;
    if (o.ttl) {
      ro.timeout = o.ttl;
    }
    if (options) {
      ro.headers = mh;
    }

    const r = await this._request(subj, data, ro);
    const pa = r as PubAckResponse;
    if (pa.stream === "") {
      throw NatsError.errorForCode(ErrorCode.INVALID_JS_ACK);
    }
    return pa;
  }

  subscribe(
    subj: string,
    opts = {} as JetStreamSubOpts,
    ...options: JetStreamSubOption[]
  ): Promise<Subscription> {
    const o = JSON.parse(JSON.stringify(opts)) as JetStreamSubOpts;
    if (options) {
      options.forEach((fn) => {
        fn(o);
      });
    }

    return Promise.reject();
  }
}

class JetStreamManagerImpl extends BaseClient implements JetStreamManager {
  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    super(nc, opts);
  }

  async getAccountInfo(): Promise<AccountInfo> {
    const r = await this._request(`${this.prefix}.INFO`);
    return r as AccountInfoResponse;
  }

  addConsumer(stream: string, cfg: ConsumerConfig): Promise<ConsumerInfo> {
    return Promise.reject();
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

  consumerInfo(stream: string, name: string): Promise<ConsumerInfo> {
    return Promise.reject();
  }

  deleteConsumer(stream: string, consumer: string): Promise<void> {
    return Promise.reject();
  }

  deleteMsg(name: string, seq: number): Promise<void> {
    return Promise.reject();
  }

  deleteStream(name: string): Promise<void> {
    return Promise.reject();
  }

  newConsumerLister(stream: string): Promise<ConsumerLister> {
    return Promise.resolve({} as ConsumerLister);
  }

  newStreamLister(): Promise<StreamLister> {
    return Promise.resolve({} as StreamLister);
  }

  purgeStream(name: string): Promise<void> {
    return Promise.reject();
  }

  async streamInfo(name: string): Promise<StreamInfo> {
    if (name === "") {
      throw new Error("name is required");
    }
    const r = await this._request(`${this.prefix}.STREAM.INFO.${name}`);
    return r as StreamInfo;
  }

  updateStream(cfg: StreamConfig): Promise<StreamInfo> {
    return Promise.reject();
  }
}

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
