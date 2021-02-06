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
import { JetStreamConfig, NatsServer } from "./helpers/launcher.ts";
import { connect } from "../src/connect.ts";
import {
  AckPolicy,
  attach,
  DeliverPolicy,
  expectLastMsgID,
  expectLastSequence,
  expectStream,
  JetStream,
  JetStreamManager,
  JsMsg,
  msgID,
} from "../nats-base-client/jsmod.ts";

import {
  assert,
  assertEquals,
  assertThrowsAsync,
} from "https://deno.land/std@0.83.0/testing/asserts.ts";
import { assertErrorCode } from "./helpers/asserts.ts";
import { Empty, ErrorCode, NatsConnection } from "../nats-base-client/mod.ts";
import { nuid } from "../nats-base-client/nuid.ts";
import { deferred } from "../nats-base-client/util.ts";
import { JsMsgImpl } from "../nats-base-client/jsmsg.ts";

async function setup(
  conf?: any,
): Promise<{ ns: NatsServer; nc: NatsConnection }> {
  const ns = await NatsServer.start(conf);
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  return { ns, nc };
}

async function cleanup(ns: NatsServer, nc: NatsConnection): Promise<void> {
  await nc.close();
  await ns.stop();
}

async function initStream(
  nc: NatsConnection,
): Promise<{ stream: string; subj: string }> {
  const jsm = await JetStreamManager(nc);
  const stream = nuid.next();
  const subj = `${stream}.A`;
  await jsm.addStream(
    { name: stream, subjects: [subj] },
  );
  return { stream, subj };
}

Deno.test("jetstream - jetstream not enabled", async () => {
  // start a regular server - no js conf
  const { ns, nc } = await setup();
  const err = await assertThrowsAsync(async () => {
    await JetStream(nc);
  });
  assertErrorCode(err, ErrorCode.JETSTREAM_NOT_ENABLED);
  await cleanup(ns, nc);
});

Deno.test("jetstream - account not enabled", async () => {
  const conf = JetStreamConfig({
    no_auth_user: "rip",
    accounts: {
      JS: {
        jetstream: "enabled",
        users: [{ user: "dlc", password: "foo" }],
      },
      IU: {
        users: [{ user: "rip", password: "bar" }],
      },
    },
  }, true);
  const { ns, nc } = await setup(conf);
  const err = await assertThrowsAsync(async () => {
    await JetStream(nc);
  });
  assertErrorCode(err, ErrorCode.JETSTREAM_NOT_ENABLED);
  await cleanup(ns, nc);
});

Deno.test("jetstream - jetstream enabled", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  await JetStream(nc);
  await cleanup(ns, nc);
});

Deno.test("jetstream - publish to non existing stream fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const js = await JetStream(nc);
  const err = await assertThrowsAsync(async () => {
    await js.publish("foo", Empty);
  });
  assertErrorCode(err, ErrorCode.NO_RESPONDERS);
  await cleanup(ns, nc);
});

Deno.test("jetstream - publish to existing stream", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { subj } = await initStream(nc);
  const njs = await JetStream(nc);
  await njs.publish(subj, Empty);
  await cleanup(ns, nc);
});

Deno.test("jetstream - expect stream", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const s1 = await initStream(nc);
  const s2 = await initStream(nc);
  const njs = await JetStream(nc);
  const err = await assertThrowsAsync(async () => {
    await njs.publish(s1.subj, Empty, expectStream(s2.stream));
  });
  assertEquals(err.message, `expected stream does not match`);
  await njs.publish(s2.subj, Empty, expectStream(s2.stream));
  await cleanup(ns, nc);
});

Deno.test("jetstream - expect last sequence", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream, subj } = await initStream(nc);
  const njs = await JetStream(nc);
  const err = await assertThrowsAsync(async () => {
    await njs.publish(subj, Empty, expectLastSequence(10));
  });
  assertEquals(err.message, "wrong last sequence: 0");

  let pa = await njs.publish(subj, Empty, expectLastSequence(0));
  assert(!pa.duplicate);
  assertEquals(pa.stream, stream);
  assertEquals(pa.seq, 1);

  pa = await njs.publish(subj, Empty, expectLastSequence(1));
  assert(!pa.duplicate);
  assertEquals(pa.stream, stream);
  assertEquals(pa.seq, 2);
  await cleanup(ns, nc);
});

Deno.test("jetstream - expect msgID", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { subj } = await initStream(nc);

  const njs = await JetStream(nc);
  await njs.publish(subj, Empty, msgID("foo"));

  const err = await assertThrowsAsync(async () => {
    await njs.publish(subj, Empty, expectLastMsgID("bar"));
  });
  assertEquals(err.message, "wrong last msg ID: foo");
  await njs.publish(subj, Empty, expectLastMsgID("foo"));
  await cleanup(ns, nc);
});

Deno.test("jetstream - direct rejects api use", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { subj } = await initStream(nc);
  const js = await JetStream(nc, { direct: true });
  await js.publish(subj, Empty);
  const err = await assertThrowsAsync(async () => {
    await js.subscribe("foo.A", {});
  });
  assertEquals(err.message, "jsm api use is not allowed on direct mode");
  await cleanup(ns, nc);
});

Deno.test("jetstream - ephemeral consumer", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream, subj } = await initStream(nc);

  const js = await JetStream(nc);
  await js.publish(subj, Empty);
  await js.publish(subj, Empty);

  const jsm = await JetStreamManager(nc);
  let consumers = await jsm.consumerLister(stream).next();
  assertEquals(consumers.length, 0);

  const done = deferred<void>();
  const sub = await js.subscribe(subj, { max: 2 });
  (async () => {
    for await (const m of sub) {
      if (sub.getProcessed() === 2) {
        done.resolve();
      }
    }
  })().then();

  consumers = await jsm.consumerLister(stream).next();
  assertEquals(consumers.length, 1);
  await done;

  consumers = await jsm.consumerLister(stream).next();
  assertEquals(consumers.length, 1);
  assertEquals(consumers[0].delivered.consumer_seq, 2);
  assertEquals(consumers[0].delivered.stream_seq, 2);
  assertEquals(consumers[0].ack_floor.stream_seq, 2);
  assertEquals(consumers[0].ack_floor.consumer_seq, 2);
  assertEquals(consumers[0].num_pending, 0);
  assertEquals(consumers[0].num_waiting, 0);
  await cleanup(ns, nc);
});

Deno.test("jetstream - ephemeral consumer manual acks", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream, subj } = await initStream(nc);

  const js = await JetStream(nc);
  await js.publish(subj, Empty);
  await js.publish(subj, Empty);

  const jsm = await JetStreamManager(nc);
  let consumers = await jsm.consumerLister(stream).next();
  assertEquals(consumers.length, 0);

  const last = deferred<JsMsg>();
  const sub = await js.subscribe(subj, { max: 2, mack: true });
  (async () => {
    for await (const m of sub) {
      if (sub.getProcessed() === 1) {
        m.ack();
      }
      if (sub.getProcessed() === 2) {
        last.resolve(m);
      }
    }
  })().then();

  consumers = await jsm.consumerLister(stream).next();
  assertEquals(consumers.length, 1);
  const m = await last;

  consumers = await jsm.consumerLister(stream).next();
  assertEquals(consumers.length, 1);
  assertEquals(consumers[0].ack_floor.stream_seq, 1);
  assertEquals(consumers[0].ack_floor.consumer_seq, 1);
  assertEquals(consumers[0].num_ack_pending, 1);
  assertEquals(consumers[0].num_pending, 0);
  assertEquals(consumers[0].num_waiting, 0);

  m.ack();
  consumers = await jsm.consumerLister(stream).next();
  assertEquals(consumers.length, 1);
  assertEquals(consumers[0].num_ack_pending, 0);

  await cleanup(ns, nc);
});

Deno.test("jetstream - attach durable consumer", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream, subj } = await initStream(nc);

  const jsm = await JetStreamManager(nc);
  await jsm.addConsumer(stream, {
    deliver_subject: "xxxx",
    deliver_policy: DeliverPolicy.All,
    durable_name: "dur",
    ack_policy: AckPolicy.Explicit,
  });

  const js = await JetStream(nc, { direct: true });
  await js.publish(subj, Empty);
  await js.publish(subj, Empty);

  let ci = await jsm.consumerInfo(stream, "dur");
  assertEquals(ci.num_pending, 2);

  const sub = await js.subscribe(
    "xxxx",
    { max: 2, mack: true },
    attach("xxxx")
  );

  console.log(sub.getSubject());
  const done = (async () => {
    for await (const jm of sub) {
      jm.ack();
    }
  })();
  await done;
  ci = await jsm.consumerInfo(stream, "dur");
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.num_ack_pending, 0);

  await cleanup(ns, nc);
});
