import { JetStreamConfig, NatsServer } from "../tests/helpers/launcher.ts";
import { connect } from "../src/connect.ts";
import { JetStream, JSM } from "../jetstream/jetstream.ts";
import {
  assert,
  assertEquals,
  assertStringIncludes,
  fail,
} from "https://deno.land/std@0.83.0/testing/asserts.ts";
import { assertErrorCode } from "../tests/helpers/mod.ts";
import { ErrorCode, StringCodec } from "../nats-base-client/mod.ts";
import { AckPolicy, JsMsg, PubAck, StreamConfig } from "../jetstream/types.ts";
import { nuid } from "../nats-base-client/nuid.ts";
import { delay } from '../nats-base-client/util.ts'

Deno.test("jsm - create", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JSM(nc);
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
    await JSM(a);
  } catch (err) {
    fail("expected a to have jetstream support");
  }

  const b = await connect(
    { port: ns.port, noResponders: true, headers: true, user: "b", pass: "b" },
  );
  try {
    await JSM(b);
    fail("expected b to fail");
  } catch (err) {
    assertErrorCode(err, ErrorCode.JETSTREAM_NOT_ENABLED);
  }

  await a.close();
  await b.close();
  await ns.stop();
});

Deno.test("jsm - stream management", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const jsm = await JSM(nc);
  try {
    const _ = await jsm.addStream({} as StreamConfig);
    fail("expected empty config to fail");
  } catch (err) {
    assertEquals(err.message, "stream name is required");
  }

  const subj = nuid.next();
  let info = await jsm.addStream({ name: subj });
  assertEquals(info.config.name, subj);

  const js = await JetStream(nc);
  const payload = StringCodec().encode("hi");
  for (let i = 1; i < 25; i++) {
    const ok = await js.publish(subj, payload);
    assertEquals(ok.stream, subj);
    assertEquals(ok.seq, i);
    assert(!ok.duplicate);
  }
  try {
    const _ = await jsm.updateStream({} as StreamConfig);
    fail("expected empty config to fail");
  } catch (err) {
    assertEquals(err.message, "stream name is required");
  }

  const oldMax = info.config.max_msgs;
  assert(oldMax !== undefined);
  info = await jsm.updateStream({ name: subj, max_msgs: oldMax + 100 });
  assertEquals(info.config.name, subj);
  assert(info.config.max_msgs !== oldMax);

  let ci = await jsm.addConsumer(
    subj,
    { durable_name: "dur", ack_policy: AckPolicy.Explicit },
  );
  assertEquals(ci.name, "dur");
  assertEquals(ci.stream_name, subj);

  try {
    await jsm.addConsumer(
      subj,
      { durable_name: "test.durable" },
    );
    fail("expected invalid durable name");
  } catch (err) {
    assertStringIncludes(err.message, "invalid durable name");
  }

  ci = await jsm.consumerInfo(subj, "dur");
  assertEquals(ci.config.durable_name, "dur");

  const lister = jsm.streamLister();
  let infos = await lister.next();
  assertEquals(infos.length, 1);
  assertEquals(infos[0].config.name, subj);
  infos = await lister.next();
  assertEquals(infos.length, 0);

  try {
    const cl = jsm.consumerLister("");
    const _ = cl.next();
    fail("should have failed");
  } catch (err) {
    assertEquals(err.message, "stream is required");
  }

  const cl = jsm.consumerLister(subj);
  let cis = await cl.next();
  assertEquals(cis.length, 1);
  assertEquals(cis[0].stream_name, subj);
  assertEquals(cis[0].config.durable_name, "dur");
  cis = await cl.next();
  assertEquals(cis.length, 0);

  await jsm.purgeStream(subj);

  let si = await jsm.streamInfo(subj);
  assertEquals(si.state.messages, 0);

  try {
    await jsm.deleteStream("");
    fail("expected delete to fail");
  } catch (err) {
    assertEquals(err.message, "stream name is required");
  }

  await jsm.deleteStream(subj);
  try {
    await jsm.streamInfo(subj);
    fail("unexpected success");
  } catch (err) {
    assertStringIncludes(err.message, "stream not found");
  }

  await nc.close();
  await ns.stop();
});

Deno.test("jetstream - subscribe", async () => {
  const ns = await NatsServer.start(JetStreamConfig({debug: true, trace: true}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true, debug: true },
  );

  const jsm = await JSM(nc);
  const js = await JetStream(nc);

  await jsm.addStream(
    { name: "foo", subjects: ["foo.A", "foo.B", "foo.C"] } as StreamConfig,
  );
  const sc = StringCodec();
  const proms: Promise<PubAck>[] = [];
  for (let i = 0; i < 5; i++) {
    proms.push(js.publish("foo.A", sc.encode("A")));
    proms.push(js.publish("foo.B", sc.encode("B")));
    proms.push(js.publish("foo.C", sc.encode("C")));
  }
  await Promise.all(proms);

  const si = await jsm.streamInfo("foo");
  assertEquals(si.state.messages, 15);

  const msgs: JsMsg[] = [];
  const sub = await js.subscribe("foo.C", { max: 5});
  const done = (async () => {
    for await(const m of sub) {
      msgs.push(m);
    }
  })();
  await done;
  assertEquals(msgs.length, 5);
  await nc.close();
  await ns.stop();
});
