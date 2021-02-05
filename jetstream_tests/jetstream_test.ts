import { JetStreamConfig, NatsServer } from "../tests/helpers/launcher.ts";
import { connect } from "../src/connect.ts";
import { JetStream, JetStreamManager } from "../jetstream/jetstream.ts";

import {
  assert,
  assertEquals,
  fail,
} from "https://deno.land/std@0.83.0/testing/asserts.ts";
import { assertErrorCode } from "../tests/helpers/asserts.ts";
import { ErrorCode, NatsError, StringCodec } from "../nats-base-client/mod.ts";
import {
  expectLastMsgID,
  expectLastSequence,
  expectStream,
  msgID,
  StorageType,
} from "../jetstream/jstypes.ts";

Deno.test("jetstream - jetstream not enabled", async () => {
  // start a regular server
  const ns = await NatsServer.start();
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );

  try {
    await JetStream(nc);
    fail("should have failed to connect to jetstream");
  } catch (err) {
    assertErrorCode(err, ErrorCode.JETSTREAM_NOT_ENABLED);
  }

  await nc.close();
  await ns.stop();
});

Deno.test("jetstream - account not enabled", async () => {
  const jso = JetStreamConfig({
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
  const ns = await NatsServer.start(jso);
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );

  try {
    await JetStream(nc);
    fail("should have failed to connect to jetstream");
  } catch (err) {
    assertErrorCode(err, ErrorCode.JETSTREAM_NOT_ENABLED);
  }
  await nc.close();
  await ns.stop();
});

Deno.test("jetstream - jetstream enabled", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );

  try {
    await JetStream(nc);
  } catch (err) {
    fail(`should have connected: ${err}`);
  }

  await nc.close();
  await ns.stop();
});

Deno.test("jetstream - publish to not existing stream", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const sc = StringCodec();
  const njs = await JetStream(nc);
  try {
    await njs.publish("foo", sc.encode("hello"));
    fail("shouldn't have succeeded");
  } catch (err) {
    assertErrorCode(err, ErrorCode.NO_RESPONDERS);
  }

  await nc.close();
  await ns.stop();
});

Deno.test("jetstream - publish to existing stream", async () => {
  const ns = await NatsServer.start(JetStreamConfig({}, true));
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  const njs = await JetStream(nc);
  const jsm = await JetStreamManager(nc);
  let si = await jsm.addStream(
    { name: "test", subjects: ["foo", "bar", "baz"] },
  );
  assertEquals(si.config.storage, StorageType.File);

  const sc = StringCodec();
  let pa = await njs.publish("foo", sc.encode("hello"));
  assertEquals(pa.stream, "test");
  assertEquals(pa.seq, 1);

  // test stream expectation
  try {
    await njs.publish("foo", sc.encode("hello"), expectStream("ORDERS"));
    fail("expected error");
  } catch (err) {
    const nerr = err as NatsError;
    assertEquals(nerr.message, "expected stream does not match");
  }
  // test last sequence expectation
  try {
    await njs.publish("foo", sc.encode("hello"), expectLastSequence(10));
    fail("expected error");
  } catch (err) {
    const nerr = err as NatsError;
    assertEquals(nerr.message, "wrong last sequence: 1");
  }

  pa = await njs.publish("foo", sc.encode("hello"), msgID("ZZZ"));
  assertEquals(pa.stream, "test");
  assertEquals(pa.seq, 2);

  pa = await njs.publish("foo", sc.encode("hello"), msgID("ZZZ"));
  assertEquals(pa.stream, "test");
  assertEquals(pa.seq, 2);
  assert(pa.duplicate);

  // test last id expectation
  try {
    await njs.publish("foo", sc.encode("hello"), expectLastMsgID("AAA"));
    fail("expected error");
  } catch (err) {
    const nerr = err as NatsError;
    assertEquals(nerr.message, "wrong last msg ID: ZZZ");
  }

  // test last sequence
  try {
    await njs.publish("foo", sc.encode("hello"), expectLastSequence(22));
    fail("expected error");
  } catch (err) {
    const nerr = err as NatsError;
    assertEquals(nerr.message, "wrong last sequence: 2");
  }

  pa = await njs.publish("foo", sc.encode("hello"), expectLastSequence(2));
  assertEquals(pa.stream, "test");
  assertEquals(pa.seq, 3);
  si = await jsm.streamInfo("test");
  assertEquals(si.state.messages, 3);
  assertEquals(si.state.last_seq, 3);

  // // test timeout
  // try {
  //   await njs.publish("foo", sc.encode("hello"), { ttl: 1 });
  //   fail("expected error");
  // } catch (err) {
  //   const nerr = err as NatsError;
  //   assertEquals(nerr.code, ErrorCode.TIMEOUT);
  // }

  await nc.close();
  await ns.stop();
});
