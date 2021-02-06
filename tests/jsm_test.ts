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
import { JetStream, JetStreamManager } from "../nats-base-client/jetstream.ts";
import {
  assert,
  assertEquals,
  assertThrows,
  assertThrowsAsync,
} from "https://deno.land/std@0.83.0/testing/asserts.ts";
import { assertErrorCode } from "../tests/helpers/mod.ts";
import { Empty, ErrorCode } from "../nats-base-client/mod.ts";
import {
  AckPolicy,
  StreamConfig,
  StreamInfo,
} from "../nats-base-client/jstypes.ts";
import { nuid } from "../nats-base-client/nuid.ts";
import {
  ConsumerNameRequired,
  StreamNameRequired,
} from "../nats-base-client/jsm.ts";
import { cleanup, initStream, setup } from "./test_util.ts";

Deno.test("jsm - jetstream not enabled", async () => {
  // start a regular server - no js conf
  const { ns, nc } = await setup();
  const err = await assertThrowsAsync(async () => {
    await JetStreamManager(nc);
  });
  assertErrorCode(err, ErrorCode.JETSTREAM_NOT_ENABLED);
  await cleanup(ns, nc);
});

Deno.test("jsm - create", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const ai = await jsm.getAccountInfo();
  assert(ai.limits.max_memory > 0);
  await cleanup(ns, nc);
});

Deno.test("jsm - account not enabled", async () => {
  const conf = {
    no_auth_user: "b",
    accounts: {
      A: {
        jetstream: "enabled",
        users: [{ user: "a", password: "a" }],
      },
      B: {
        users: [{ user: "b" }],
      },
    },
  };
  const { ns, nc } = await setup(JetStreamConfig(conf, true));
  const err = await assertThrowsAsync(async () => {
    await JetStreamManager(nc);
  });
  assertErrorCode(err, ErrorCode.JETSTREAM_NOT_ENABLED);

  const a = await connect(
    { port: ns.port, noResponders: true, headers: true, user: "a", pass: "a" },
  );
  await JetStreamManager(a);
  await a.close();
  await cleanup(ns, nc);
});

Deno.test("jsm - empty stream config fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const err = await assertThrowsAsync(async () => {
    await jsm.addStream({} as StreamConfig);
  });
  assertEquals(err.message, StreamNameRequired);
  await cleanup(ns, nc);
});

Deno.test("jsm - empty stream config update fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const name = nuid.next();
  let ci = await jsm.addStream({ name: name, subjects: [`${name}.>`] });
  assertEquals(ci!.config!.subjects!.length, 1);

  const err = await assertThrowsAsync(async () => {
    await jsm.updateStream({} as StreamConfig);
  });
  ci!.config!.subjects!.push("foo");
  ci = await jsm.updateStream(ci.config);
  assertEquals(ci!.config!.subjects!.length, 2);
  await cleanup(ns, nc);
});

Deno.test("jsm - delete empty stream name fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const err = await assertThrowsAsync(async () => {
    await jsm.deleteStream("");
  });
  assertEquals(err.message, StreamNameRequired);
  await cleanup(ns, nc);
});

Deno.test("jsm - info empty stream name fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const err = await assertThrowsAsync(async () => {
    await jsm.streamInfo("");
  });
  assertEquals(err.message, StreamNameRequired);
  await cleanup(ns, nc);
});

Deno.test("jsm - info msg not found stream name fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const name = nuid.next();
  const err = await assertThrowsAsync(async () => {
    await jsm.streamInfo(name);
  });
  assertEquals(err.message, "stream not found");
  await cleanup(ns, nc);
});

Deno.test("jsm - delete msg empty stream name fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const err = await assertThrowsAsync(async () => {
    await jsm.deleteMsg("", 1);
  });
  assertEquals(err.message, StreamNameRequired);
  await cleanup(ns, nc);
});

Deno.test("jsm - delete msg not found stream name fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const name = nuid.next();
  const err = await assertThrowsAsync(async () => {
    await jsm.deleteMsg(name, 1);
  });
  assertEquals(err.message, "stream not found");
  await cleanup(ns, nc);
});

Deno.test("jsm - no stream lister is empty", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const streams = await jsm.streamLister().next();
  assertEquals(streams.length, 0);
  await cleanup(ns, nc);
});

Deno.test("jsm - add stream", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const name = nuid.next();
  let si = await jsm.addStream({ name });
  assertEquals(si.config.name, name);

  const fn = (i: StreamInfo): boolean => {
    assertEquals(i.config, si.config);
    assertEquals(i.state, si.state);
    assertEquals(i.created, si.created);
    return true;
  };

  fn(await jsm.streamInfo(name));
  let lister = await jsm.streamLister().next();
  fn(lister[0]);

  // add some data
  const js = await JetStream(nc);
  const ok = await js.publish(name, Empty);
  assertEquals(ok.stream, name);
  assertEquals(ok.seq, 1);
  assert(!ok.duplicate);

  si = await jsm.streamInfo(name);
  lister = await jsm.streamLister().next();
  fn(lister[0]);

  await cleanup(ns, nc);
});

Deno.test("jsm - purge not found stream name fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const name = nuid.next();
  const err = await assertThrowsAsync(async () => {
    await jsm.purgeStream(name);
  });
  assertEquals(err.message, "stream not found");
  await cleanup(ns, nc);
});

Deno.test("jsm - purge empty stream name fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const err = await assertThrowsAsync(async () => {
    await jsm.purgeStream("");
  });
  assertEquals(err.message, StreamNameRequired);
  await cleanup(ns, nc);
});

Deno.test("jsm - stream purge", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await JetStreamManager(nc);

  const js = await JetStream(nc);
  const ok = await js.publish(subj, Empty);
  assertEquals(ok.seq, 1);

  let si = await jsm.streamInfo(stream);
  assertEquals(si.state.messages, 1);

  await jsm.purgeStream(stream);
  si = await jsm.streamInfo(stream);
  assertEquals(si.state.messages, 0);

  await cleanup(ns, nc);
});

Deno.test("jsm - stream delete", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await JetStreamManager(nc);

  const js = await JetStream(nc);
  const ok = await js.publish(subj, Empty);
  assertEquals(ok.seq, 1);
  await jsm.deleteStream(stream);
  const err = await assertThrowsAsync(async () => {
    await jsm.streamInfo(stream);
  });
  assertEquals(err.message, "stream not found");
  await cleanup(ns, nc);
});

Deno.test("jsm - stream delete message", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await JetStreamManager(nc);

  const js = await JetStream(nc);
  const ok = await js.publish(subj, Empty);
  assertEquals(ok.seq, 1);

  let si = await jsm.streamInfo(stream);
  assertEquals(si.state.messages, 1);
  assertEquals(si.state.first_seq, 1);
  assertEquals(si.state.last_seq, 1);

  assert(await jsm.deleteMsg(stream, 1));
  si = await jsm.streamInfo(stream);
  assertEquals(si.state.messages, 0);
  assertEquals(si.state.first_seq, 2);
  assertEquals(si.state.last_seq, 1);

  await cleanup(ns, nc);
});

Deno.test("jsm - consumer info on empty stream name fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const err = await assertThrowsAsync(async () => {
    await jsm.consumerInfo("", "");
  });
  assertEquals(err.message, StreamNameRequired);
  await cleanup(ns, nc);
});

Deno.test("jsm - consumer info on empty consumer name fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const err = await assertThrowsAsync(async () => {
    await jsm.consumerInfo("foo", "");
  });
  assertEquals(err.message, ConsumerNameRequired);
  await cleanup(ns, nc);
});

Deno.test("jsm - consumer info on not found stream fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const err = await assertThrowsAsync(async () => {
    await jsm.consumerInfo("foo", "dur");
  });
  assertEquals(err.message, "stream not found");
  await cleanup(ns, nc);
});

Deno.test("jsm - consumer info on not found consumer", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream } = await initStream(nc);
  const jsm = await JetStreamManager(nc);
  const err = await assertThrowsAsync(async () => {
    await jsm.consumerInfo(stream, "dur");
  });
  assertEquals(err.message, "consumer not found");
  await cleanup(ns, nc);
});

Deno.test("jsm - consumer info", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream } = await initStream(nc);
  const jsm = await JetStreamManager(nc);
  await jsm.addConsumer(
    stream,
    { durable_name: "dur", ack_policy: AckPolicy.Explicit },
  );
  const ci = await jsm.consumerInfo(stream, "dur");
  assertEquals(ci.name, "dur");
  assertEquals(ci.config.durable_name, "dur");
  assertEquals(ci.config.ack_policy, AckPolicy.Explicit);
  await cleanup(ns, nc);
});

Deno.test("jsm - no consumer lister with empty stream fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const err = assertThrows(() => {
    jsm.consumerLister("");
  });
  assertEquals(err.message, StreamNameRequired);
  await cleanup(ns, nc);
});

Deno.test("jsm - no consumer lister with no consumers empty", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream } = await initStream(nc);
  const jsm = await JetStreamManager(nc);
  const consumers = await jsm.consumerLister(stream).next();
  assertEquals(consumers.length, 0);
  await cleanup(ns, nc);
});

Deno.test("jsm - lister", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream } = await initStream(nc);
  const jsm = await JetStreamManager(nc);
  await jsm.addConsumer(
    stream,
    { durable_name: "dur", ack_policy: AckPolicy.Explicit },
  );
  let consumers = await jsm.consumerLister(stream).next();
  assertEquals(consumers.length, 1);
  assertEquals(consumers[0].config.durable_name, "dur");

  await jsm.deleteConsumer(stream, "dur");
  consumers = await jsm.consumerLister(stream).next();
  assertEquals(consumers.length, 0);

  await cleanup(ns, nc);
});

Deno.test("jsm - update stream", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream } = await initStream(nc);
  const jsm = await JetStreamManager(nc);

  let si = await jsm.streamInfo(stream);
  assertEquals(si.config!.subjects!.length, 1);

  si.config!.subjects!.push("foo");
  si = await jsm.updateStream(si.config);
  assertEquals(si.config!.subjects!.length, 2);
  await cleanup(ns, nc);
});
