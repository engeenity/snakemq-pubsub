## -*- coding: utf-8 -*-\
"""
    snakemq_pubsub.tests.__init__
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Tests for snakeMQ Pub/Sub.

    :copyright: (c) 2015 by Nicholas Repole and contributors.
                See AUTHORS for more details.
    :license: MIT - See LICENSE for more details.
"""
from __future__ import unicode_literals
import unittest
from snakemq_pubsub import Publisher, Subscriber, Broker
import threading
import time
import warnings


class SnakeMQPubSubTests(unittest.TestCase):

    """A collection of snakeMQ-PubSub tests."""

    def setUp(self):
        """Start publisher, subscriber, and broker loops."""
        self.subscriber1_lock = threading.RLock()
        self.subscriber1_messages = list()
        self.subscriber2_lock = threading.RLock()
        self.subscriber2_messages = list()
        self.broker = Broker("localhost", 4000, "broker")
        self.publisher = Publisher("localhost", 4000, "broker", "publisher")
        self.subscriber1 = Subscriber("localhost", 4000, "broker",
                                      "subscriber1", self.subscriber1_onrecv)
        self.subscriber2 = Subscriber("localhost", 4000, "broker",
                                      "subscriber2", self.subscriber2_onrecv)
        self.broker_thread = threading.Thread(target=self.broker.run)
        self.broker_thread.start()
        self.publisher_thread = threading.Thread(target=self.publisher.run)
        self.publisher_thread.start()
        self.subscriber1_thread = threading.Thread(target=self.subscriber1.run)
        self.subscriber1_thread.start()
        self.subscriber2_thread = threading.Thread(target=self.subscriber2.run)
        self.subscriber2_thread.start()

    def tearDown(self):
        """Stop all message queue loops."""
        self.publisher.stop()
        self.subscriber1.stop()
        self.subscriber2.stop()
        self.broker.stop()
        self.broker_thread.join()
        self.subscriber1_thread.join()
        self.subscriber2_thread.join()
        self.publisher_thread.join()
        with warnings.catch_warnings():
            # These create warnings for sockets not closing.
            # Above "with" doesn't seem to work in suppressing them.
            self.publisher.link.cleanup()
            self.subscriber1.link.cleanup()
            self.subscriber2.link.cleanup()
            self.broker.link.cleanup()
        time.sleep(5)

    def subscriber1_onrecv(self, conn, ident, message):
        """Place subscriber1 received messages in a list."""
        msg_text = message.data.decode("utf-8")
        self.subscriber1_lock.acquire()
        try:
            self.subscriber1_messages.append(msg_text)
        finally:
            self.subscriber1_lock.release()

    def subscriber2_onrecv(self, conn, ident, message):
        """Place subscriber2 received messages in a list."""
        msg_text = message.data.decode("utf-8")
        self.subscriber2_lock.acquire()
        try:
            self.subscriber2_messages.append(msg_text)
        finally:
            self.subscriber2_lock.release()

    def test_unsubscribe(self):
        """Test that simple unsubscription works."""
        self.subscriber1.subscribe("a")
        # wait to make sure subscriptions are done
        time.sleep(4)
        self.publisher.publish("a", "a")
        time.sleep(4)
        self.subscriber1.unsubscribe("a")
        time.sleep(4)
        self.publisher.publish("a", "a2")
        # wait to make sure publishing is done
        time.sleep(4)
        self.assertTrue("a" in self.subscriber1_messages)
        self.assertTrue("a2" not in self.subscriber1_messages)

    def test_subscribe(self):
        """Test simple pub/sub subscription."""
        self.subscriber1.subscribe("a")
        # wait to make sure subscriptions are done
        time.sleep(4)
        self.publisher.publish("a", "a")
        self.publisher.publish("b", "b")
        # wait to make sure publishing is done
        time.sleep(4)
        self.assertTrue("a" in self.subscriber1_messages)
        self.assertTrue("b" not in self.subscriber1_messages)

    def test_basic_pub_sub(self):
        """Make sure published messages go to the right subscribers."""
        self.subscriber1.subscribe("a")
        self.subscriber2.subscribe("b")
        self.subscriber1.subscribe("c")
        self.subscriber2.subscribe("c")
        # wait to make sure subscriptions are done
        time.sleep(4)
        self.publisher.publish("a", "a")
        self.publisher.publish("b", "b")
        self.publisher.publish("c", "c")
        # wait to make sure publishing is done
        time.sleep(4)
        self.assertTrue("a" in self.subscriber1_messages)
        self.assertTrue("a" not in self.subscriber2_messages)
        self.assertTrue("b" in self.subscriber2_messages)
        self.assertTrue("b" not in self.subscriber1_messages)
        self.assertTrue("c" in self.subscriber1_messages)
        self.assertTrue("c" in self.subscriber2_messages)

    def test_broker_crash_reconnect(self):
        """Make sure we can recover from a broker crash."""
        self.subscriber1.subscribe("a")
        # wait to make sure subscription sticks
        time.sleep(4)
        self.publisher.publish("a", "a")
        time.sleep(4)
        self.broker.stop()
        self.broker_thread.join()
        self.broker.link.cleanup()
        time.sleep(4)
        self.publisher.publish("a", "a2")
        self.broker = Broker("localhost", 4000, "broker")
        self.broker_thread = threading.Thread(target=self.broker.run)
        self.broker_thread.start()
        self.publisher.publish("a", "a3")
        time.sleep(4)
        self.assertTrue("a" in self.subscriber1_messages)
        self.assertTrue("a2" in self.subscriber1_messages)
        self.assertTrue("a3" in self.subscriber1_messages)

    def test_subscriber_reconnect(self):
        """Make sure a subscriber can periodically reconnect."""
        self.subscriber1.subscribe("a")
        # wait to make sure subscription sticks
        time.sleep(4)
        self.publisher.publish("a", "a")
        time.sleep(4)
        self.subscriber1.stop()
        self.subscriber1_thread.join()
        self.publisher.publish("a", "a2")
        self.subscriber1.run(5)
        time.sleep(4)
        self.assertTrue("a" in self.subscriber1_messages)
        self.assertTrue("a2" in self.subscriber1_messages)

    def test_subscriber_disconnect(self):
        """Make sure broker channel subscriptions get cleaned up."""
        self.subscriber1.subscribe("a")
        # wait to make sure subscription sticks
        time.sleep(4)
        old_sub_count = len(self.broker.channel_subscribers.get("a"))
        self.broker.stop()
        self.broker_thread.join()
        self.broker.link.cleanup()
        time.sleep(4)
        self.assertTrue(old_sub_count == 1)
        self.assertTrue(len(self.broker.channel_subscribers.get("a")) == 0)

if __name__ == '__main__':    # pragma: no cover
    unittest.main()
