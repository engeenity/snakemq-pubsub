"""
    snakemq_pubsub.__init__
    ~~~~~~~~~~~~~~~~~~~~~~~

    A simple implementation of a PubSub broker for snakeMQ.

    :copyright: (c) 2015 by Nicholas Repole and contributors.
                See AUTHORS for more details.
    :license: MIT - See LICENSE for more details.
"""
import os
import snakemq.link
import snakemq.packeter
import snakemq.messaging
import snakemq.message
import threading

__version__ = "0.1.0.dev0"

class Broker(object):

    """A snakeMQ Pub/Sub broker."""

    # Class level variable.
    # Dict of channel names where each key is a channel name which
    # maps to a set of subscriber identities.
    channels = {}

    def __init__(self, broker_host, broker_port, broker_identity):
        self.broker_identity = broker_identity
        self.link = snakemq.link.Link()
        self.packeter = snakemq.packeter.Packeter(self.link)
        self.messaging = snakemq.messaging.Messaging(
            self.broker_identity, "", self.packeter)
        self.messaging.on_message_recv.add(self.on_recv)
        self.messaging.on_message_drop.add(self.on_drop)
        self.link.add_listener((broker_host, broker_port))

    def on_recv(self, conn, ident, message):
        """Handle PUBLISH, SUBSCRIBE, and UNSUBSCRIBE commands."""
        msg_text = message.data.decode("utf-8")
        if msg_text.startswith("SUBSCRIBE "):
            msg_split = msg_text.split(" ")
            for i, arg in enumerate(msg_split):
                if i != 0:
                    channel = arg
                    if channel not in self.channels:
                        self.channels[channel] = set()
                    self.channels[channel].add(ident)
        elif msg_text.startswith("UNSUBSCRIBE "):
            msg_split = msg_text.split(" ")
            for i, arg in enumerate(msg_split):
                if i != 0:
                    channel = arg
                    if channel in self.channels:
                        self.channels[channel].remove(ident)
        elif msg_text.startswith("PUBLISH "):
            msg_split = msg_text.split(" ")
            if len(msg_split) >= 3:
                channel = msg_split[1]
                pub_msg_text = " ".join(msg_split[2:])
                if channel in self.channels:
                    for connection in self.channels[channel]:
                        pub_msg = snakemq.message.Message(
                            pub_msg_text.encode("utf-8"), ttl=60)
                        self.messaging.send_message(connection, pub_msg)

    def on_drop(self, ident, message):
        """Handle a dropped message.

        Probably want to do some logging here. Override this to do so.

        """
        pass

    def run(self):
        """Run the snakeMQ stack and listen for messages."""
        self.link.loop()

    def stop(self):
        """Stop the message broker."""
        self.link.stop()


class Publisher(threading.Thread):

    """Manages publishing messages to a message broker."""

    def __init__(self, broker_host, broker_port, broker_identity, my_identity):
        """Create a connection able to publish to the broker."""
        threading.Thread.__init__(self)
        self.broker_identity = broker_identity
        self.my_identity = my_identity
        self.link = snakemq.link.Link()
        self.packeter = snakemq.packeter.Packeter(self.link)
        self.messaging = snakemq.messaging.Messaging(
            self.my_identity, "", self.packeter)
        self.link.add_connector((broker_host, broker_port))

    def publish(self, channel, message):
        """Publish a message on the supplied channel."""
        command = "PUBLISH {channel} {message}".format(
            channel=channel, message=message)
        command_msg = snakemq.message.Message(command.encode("utf-8"), ttl=60)
        self.messaging.send_message(self.broker_identity, command_msg)

    def run(self):
        """Run the snakeMQ stack to allow publishing."""
        self.link.loop()

    def stop(self):
        """Stop the publisher."""
        self.link.stop()


class Subscriber(threading.Thread):

    """A snakeMQ Pub/Sub subscriber."""

    def __init__(self, broker_host, broker_port, broker_identity, my_identity,
                 on_recv):
        """Create a subscriber that listens on subscribed channels."""
        threading.Thread.__init__(self)
        self.broker_identity = broker_identity
        self.my_identity = my_identity
        self.link = snakemq.link.Link()
        self.packeter = snakemq.packeter.Packeter(self.link)
        self.messaging = snakemq.messaging.Messaging(
            self.my_identity, "", self.packeter)
        self.messaging.on_message_recv.add(on_recv)
        self.link.add_connector((broker_host, broker_port))
        self.link.on_connect.add(self.on_connect)
        self.subscriptions = set()

    def subscribe(self, channel):
        """Subscribe to the provided channel."""
        command = "SUBSCRIBE {channel}".format(channel=channel)
        command_msg = snakemq.message.Message(command.encode("utf-8"), ttl=60)
        self.messaging.send_message(self.broker_identity, command_msg)
        self.subscriptions.add(channel)
        print("Subscribed to {channel}".format(channel=channel))

    def unsubscribe(self, channel):
        """Unsubscribe from the provided channel."""
        command = "UNSUBSCRIBE {channel}".format(channel=channel)
        command_msg = snakemq.message.Message(command.encode("utf-8"), ttl=60)
        self.messaging.send_message(self.broker_identity, command_msg)
        self.subscriptions.remove(channel)

    def on_connect(self, *args, **kwargs):
        """Resubscribe to any prior subscriptions."""
        for channel in self.subscriptions:
            self.subscribe(channel)

    def run(self):
        """Begin listening for messages."""
        self.link.loop()

    def stop(self):
        """Stop listening for messages."""
        self.link.stop()
