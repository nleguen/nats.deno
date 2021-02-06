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
  fail,
} from "https://deno.land/std@0.83.0/testing/asserts.ts";
import { assertErrorCode } from "../tests/helpers/mod.ts";
import { Empty, ErrorCode, StringCodec } from "../nats-base-client/mod.ts";
import {
  AckPolicy,
  JsMsg,
  ns as nanos,
  PubAck,
  StreamConfig,
  StreamInfo,
} from "../nats-base-client/jstypes.ts";
import { nuid } from "../nats-base-client/nuid.ts";
import {
  ConsumerNameRequired,
  StreamNameRequired,
} from "../nats-base-client/jsm.ts";
import { delay } from "../nats-base-client/util.ts";

Deno.test("jsm - create", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JetStreamManager(nc);
  const ai = await jsm.getAccountInfo();
  assert(ai.limits.max_memory > 0);
  await nc.close();
  await ns.stop();
});

Deno.test("jsm - account not enabled", async () => {
  const ns = await NatsServer.start(JetStreamConfig({
    no_auth_user: "c",
    accounts: {
      A: {
        jetstream: "enabled",
        users: [{ user: "a", password: "a" }],
      },
      B: {
        users: [{ user: "b", password: "b" }, { user: "c" }],
      },
    },
  }, true));

  const a = await connect(
    { port: ns.port, noResponders: true, headers: true, user: "a", pass: "a" },
  );
  try {
    await JetStreamManager(a);
  } catch (err) {
    fail("expected a to have jetstream support");
  }

  const b = await connect(
    { port: ns.port, noResponders: true, headers: true, user: "b", pass: "b" },
  );
  try {
    await JetStreamManager(b);
    fail("expected b to fail");
  } catch (err) {
    assertErrorCode(err, ErrorCode.JETSTREAM_NOT_ENABLED);
  }

  await a.close();
  await b.close();
  await ns.stop();
});

Deno.test("jsm - empty stream config fails", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JetStreamManager(nc);
  try {
    const _ = await jsm.addStream({} as StreamConfig);
    fail("expected empty config to fail");
  } catch (err) {
    assertEquals(err.message, StreamNameRequired);
  }
  await nc.close();
  await ns.stop();
});

Deno.test("jsm - empty stream config update fails", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JetStreamManager(nc);

  const subj = nuid.next();
  let info = await jsm.addStream({ name: subj });
  assertEquals(info.config.name, subj);

  try {
    const _ = await jsm.updateStream({} as StreamConfig);
    fail("expected empty config to fail");
  } catch (err) {
    assertEquals(err.message, StreamNameRequired);
  }
  await nc.close();
  await ns.stop();
});

Deno.test("jsm - delete stream not found stream fails", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JetStreamManager(nc);
  try {
    await jsm.deleteStream("");
    fail("expected empty name to fail");
  } catch (err) {
    assertEquals(err.message, StreamNameRequired);
  }
  await nc.close();
  await ns.stop();
});

Deno.test("jsm - purge stream not found stream fails", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JetStreamManager(nc);
  try {
    await jsm.purgeStream("");
    fail("expected empty name to fail");
  } catch (err) {
    assertEquals(err.message, StreamNameRequired);
  }
  await nc.close();
  await ns.stop();
});

Deno.test("jsm - stream info empty name fails", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JetStreamManager(nc);
  try {
    await jsm.streamInfo("");
    fail("expected empty name to fail");
  } catch (err) {
    assertEquals(err.message, StreamNameRequired);
  }
  await nc.close();
  await ns.stop();
});

Deno.test("jsm - stream info delete message with empty name fails", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JetStreamManager(nc);
  try {
    await jsm.deleteMsg("", 1);
    fail("expected empty name to fail");
  } catch (err) {
    assertEquals(err.message, StreamNameRequired);
  }
  await nc.close();
  await ns.stop();
});

Deno.test("jsm - stream info not found fails", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JetStreamManager(nc);
  try {
    await jsm.streamInfo(nuid.next());
    fail("expected not found stream to fail");
  } catch (err) {
    assertEquals(err.message, "stream not found");
  }
  await nc.close();
  await ns.stop();
});

Deno.test("jsm - no streams returns empty lister", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JetStreamManager(nc);
  const lister = jsm.streamLister();
  const infos = await lister.next();
  assertEquals(infos.length, 0);

  await nc.close();
  await ns.stop();
});

Deno.test("jsm - create stream", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
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

  await nc.close();
  await ns.stop();
});

Deno.test("jsm - stream purge", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JetStreamManager(nc);
  const name = nuid.next();
  await jsm.addStream({ name });

  const js = await JetStream(nc);
  const ok = await js.publish(name, Empty);
  assertEquals(ok.seq, 1);

  let si = await jsm.streamInfo(name);
  assertEquals(si.state.messages, 1);

  await jsm.purgeStream(name);
  si = await jsm.streamInfo(name);
  assertEquals(si.state.messages, 0);

  await nc.close();
  await ns.stop();
});

Deno.test("jsm - stream delete", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JetStreamManager(nc);
  const name = nuid.next();
  await jsm.addStream({ name });
  await jsm.streamInfo(name);
  await jsm.deleteStream(name);

  try {
    await jsm.streamInfo(name);
    fail("expected not found stream to fail");
  } catch (err) {
    assertEquals(err.message, "stream not found");
  }
  await nc.close();
  await ns.stop();
});

Deno.test("jsm - stream delete message", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JetStreamManager(nc);
  const name = nuid.next();
  await jsm.addStream({ name });

  const js = await JetStream(nc);
  const ok = await js.publish(name, Empty);
  assertEquals(ok.seq, 1);

  let si = await jsm.streamInfo(name);
  assertEquals(si.state.messages, 1);
  assertEquals(si.state.first_seq, 1);
  assertEquals(si.state.last_seq, 1);

  assert(await jsm.deleteMsg(name, 1));
  si = await jsm.streamInfo(name);
  assertEquals(si.state.messages, 0);
  assertEquals(si.state.first_seq, 2);
  assertEquals(si.state.last_seq, 1);

  await nc.close();
  await ns.stop();
});

Deno.test("jsm - consumer info on empty stream name fails", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JetStreamManager(nc);

  try {
    const _ = await jsm.consumerInfo("", "");
    fail("expected empty stream name to fail");
  } catch (err) {
    assertEquals(err.message, StreamNameRequired);
  }
  await nc.close();
  await ns.stop();
});

Deno.test("jsm - consumer info on empty consumer name fails", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JetStreamManager(nc);

  try {
    const _ = await jsm.consumerInfo("somestream", "");
    fail("expected empty consumer name to fail");
  } catch (err) {
    assertEquals(err.message, ConsumerNameRequired);
  }
  await nc.close();
  await ns.stop();
});

Deno.test("jsm - consumer info on not found stream", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JetStreamManager(nc);

  try {
    const _ = await jsm.consumerInfo("somestream", "consumer");
    fail("expected stream not found");
  } catch (err) {
    assertEquals(err.message, "stream not found");
  }
  await nc.close();
  await ns.stop();
});

Deno.test("jsm - consumer info on not found consumer", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JetStreamManager(nc);
  const name = nuid.next();
  await jsm.addStream({ name });

  try {
    const _ = await jsm.consumerInfo(name, "consumer");
    fail("expected stream not found");
  } catch (err) {
    assertEquals(err.message, "consumer not found");
  }
  await nc.close();
  await ns.stop();
});

Deno.test("jsm - consumer info", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JetStreamManager(nc);
  const name = nuid.next();
  await jsm.addStream({ name });
  await jsm.addConsumer(
    name,
    { durable_name: "dur", ack_policy: AckPolicy.Explicit },
  );

  const ci = await jsm.consumerInfo(name, "dur");
  assertEquals(ci.name, "dur");
  assertEquals(ci.config.durable_name, "dur");
  assertEquals(ci.config.ack_policy, AckPolicy.Explicit);

  await nc.close();
  await ns.stop();
});

Deno.test("jsm - consumer lister with empty stream fails", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JetStreamManager(nc);
  try {
    await jsm.consumerLister("");
    fail("expected stream name required");
  } catch (err) {
    assertEquals(err.message, StreamNameRequired);
  }
  await nc.close();
  await ns.stop();
});

Deno.test("jsm - consumer add/delete/list", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JetStreamManager(nc);
  const name = nuid.next();
  await jsm.addStream({ name });
  await jsm.addConsumer(
    name,
    { durable_name: "dur", ack_policy: AckPolicy.Explicit },
  );

  let lister = await jsm.consumerLister(name);
  let consumers = await lister.next();
  assertEquals(consumers.length, 1);
  assertEquals(consumers[0].name, "dur");

  await jsm.deleteConsumer(name, "dur");
  lister = await jsm.consumerLister(name);
  consumers = await lister.next();
  assertEquals(consumers.length, 0);

  await nc.close();
  await ns.stop();
});

Deno.test("jsm - update stream", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JetStreamManager(nc);
  const name = nuid.next();
  let info = await jsm.addStream({ name });
  const max = info.config.max_msgs;

  const oldMax = info.config.max_msgs || 0;
  info = await jsm.updateStream({ name: name, max_msgs: oldMax + 100 });
  assertEquals(info.config.name, name);
  assert(info.config.max_msgs !== oldMax);
  assertEquals(info.config.max_msgs, oldMax + 100);
  await nc.close();
  await ns.stop();
});
