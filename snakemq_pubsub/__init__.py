"""
    snakemq_pubsub.__init__
    ~~~~~~~~~~~~~~~~~~~~~~~

    An implementation of the publish-subscribe pattern for snakeMQ.

    :copyright: (c) 2015 by Nicholas Repole and contributors.
                See AUTHORS for more details.
    :license: MIT - See LICENSE for more details.
"""
from __future__ import unicode_literals
import logging
import snakemq.link
import snakemq.packeter
import snakemq.messaging
import snakemq.message
import threading

logger = logging.getLogger("snakemq_pubsub")
__version__ = "0.1.0"


class MQConnectorMixin(object):

    """One end of a snakeMQ connection.

    Broker, Subscriber, and Publisher all inherit from this class.

    """

    def __init__(self, identity):
        """Set up messaging and a link."""
        self.identity = identity
        self.link = snakemq.link.Link()
        self.packeter = snakemq.packeter.Packeter(self.link)
        self.messaging = snakemq.messaging.Messaging(
            self.identity, "", self.packeter)
        self.link.on_connect.add(self.link_on_connect)
        self.link.on_disconnect.add(self.link_on_disconnect)
        self.messaging.on_connect.add(self.messenger_on_connect)
        self.messaging.on_disconnect.add(self.messenger_on_disconnect)
        self.messaging.on_message_recv.add(self.on_recv)
        self.messaging.on_message_drop.add(self.on_drop)

    def link_on_connect(self, conn):
        """Log when the link connects."""
        logger.info("LINK: {my_ident} connected to {conn}".format(
            my_ident=self.identity, conn=conn))

    def link_on_disconnect(self, conn):
        """Log when the link disconnects."""
        logger.info("LINK: {my_ident} disconnected from {conn}".format(
            my_ident=self.identity, conn=conn))

    def messenger_on_connect(self, conn, ident):
        """Log any new messenger connections."""
        logger.info(
            "MESSENGER: {my_ident} connected to {conn} ({ident}).".format(
                my_ident=self.identity,
                conn=conn,
                ident=ident))

    def messenger_on_disconnect(self, conn, ident):
        """Log any new new messenger disconnections."""
        logger.info(
            "MESSENGER: {my_ident} disconnected from {conn} ({ident}).".format(
                my_ident=self.identity,
                conn=conn,
                ident=ident))

    def on_recv(self, conn, ident, message):
        """Handle an incoming message.

        Will typically be overriden by an inheriting class.

        """
        msg_text = message.data.decode("utf-8")
        logger.debug("Message received: {message}".format(message=msg_text))

    def on_drop(self, ident, message):
        """Handle a dropped message."""
        logger.info("Dropped message.")  # pragma: no cover

    def run(self, runtime=None):
        """Run the snakeMQ stack and listen for messages."""
        self.link.loop(runtime=runtime)

    def stop(self):
        """Stop the message broker."""
        self.link.stop()


class BrokerClient(MQConnectorMixin):

    """Client connection to a broker server.

    Publisher and Subscriber inherit from this class.

    """

    def __init__(self, broker_host, broker_port, broker_identity, identity):
        """Initialize a connection to a broker."""
        super(BrokerClient, self).__init__(identity)
        self.is_connected = False
        self.broker_identity = broker_identity
        self.link.add_connector((broker_host, broker_port))


class Broker(MQConnectorMixin):

    """A snakeMQ Pub/Sub broker."""

    def __init__(self, host, port, identity):
        """Initialize the broker listener."""
        super(Broker, self).__init__(identity)
        self.link.add_listener((host, port))
        # Dict where each key is a channel name which maps to a set of
        # connection identities.
        self.channel_subscribers = {}
        # Dict where each key is a connection name which maps to a set of
        # subscription names.
        self.connection_subscriptions = {}

    def link_on_disconnect(self, conn):
        """Remove any remaining subscriptions on disconnect."""
        super(Broker, self).link_on_disconnect(conn)
        subscriptions = self.connection_subscriptions.get(conn)
        if subscriptions:
            for channel in subscriptions:
                subscribers = self.channel_subscribers.get(channel)
                if subscribers:
                    subscribers.discard(conn)

    def on_recv(self, conn, ident, message):
        """Handle PUBLISH, SUBSCRIBE, and UNSUBSCRIBE commands."""
        msg_text = message.data.decode("utf-8")
        logger.debug("Message received: {message}".format(message=msg_text))
        if msg_text.startswith("SUBSCRIBE "):
            msg_split = msg_text.split(" ")
            for i, arg in enumerate(msg_split):
                if i != 0:
                    channel = arg
                    if conn not in self.connection_subscriptions:
                        self.connection_subscriptions[conn] = set()
                    if channel not in self.channel_subscribers:
                        self.channel_subscribers[channel] = set()
                    self.channel_subscribers[channel].add(conn)
                    self.connection_subscriptions[conn].add(channel)
                    logger.info("{ident} subscribed to {channel}".format(
                        ident=ident,
                        channel=channel))
        elif msg_text.startswith("UNSUBSCRIBE "):
            msg_split = msg_text.split(" ")
            for i, arg in enumerate(msg_split):
                if i != 0:
                    channel = arg
                    if channel in self.channel_subscribers:
                        self.channel_subscribers[channel].remove(conn)
                        self.connection_subscriptions[conn].remove(channel)
                        logger.info(
                            "{ident} unsubscribed from {channel}".format(
                                ident=ident,
                                channel=channel))
        elif msg_text.startswith("PUBLISH "):
            msg_split = msg_text.split(" ")
            if len(msg_split) >= 3:
                channel = msg_split[1]
                pub_msg_text = " ".join(msg_split[2:])
                if channel in self.channel_subscribers:
                    for subscriber_conn in self.channel_subscribers[channel]:
                        subscriber_ident = self.messaging._ident_by_conn.get(
                            subscriber_conn)
                        if subscriber_ident is not None:
                            pub_msg = snakemq.message.Message(
                                pub_msg_text.encode("utf-8"), ttl=60)
                            self.messaging.send_message(subscriber_ident,
                                                        pub_msg)
                    logger.debug(
                        "Message published to {count} subscribers "
                        "on channel {channel}".format(
                            channel=channel,
                            count=len(self.channel_subscribers[channel])))


class Publisher(BrokerClient):

    """Manages publishing messages to a message broker."""

    def publish(self, channel, message):
        """Publish a message on the supplied channel."""
        command = "PUBLISH {channel} {message}".format(
            channel=channel, message=message)
        command_msg = snakemq.message.Message(command.encode("utf-8"), ttl=60)
        self.messaging.send_message(self.broker_identity, command_msg)
        logger.info("Publishing message to {channel}".format(channel=channel))


class Subscriber(BrokerClient):

    """A snakeMQ Pub/Sub subscriber."""

    def __init__(self, broker_host, broker_port, broker_identity, identity,
                 on_recv):
        """Create a subscriber that listens on subscribed channels."""
        super(Subscriber, self).__init__(
            broker_host, broker_port, broker_identity, identity)
        self.subscriptions = set()
        self.subscriptions_lock = threading.RLock()
        self.on_recv_callback = on_recv

    def subscribe(self, channel):
        """Subscribe to the provided channel."""
        command = "SUBSCRIBE {channel}".format(channel=channel)
        command_msg = snakemq.message.Message(command.encode("utf-8"), ttl=60)
        if self.is_connected:
            # if not connected, subscription will be finalized later.
            self.messaging.send_message(self.broker_identity, command_msg)
            logger.info("Subscribing {ident} to {channel}".format(
                ident=self.identity, channel=channel))
        self.subscriptions_lock.acquire()
        try:
            self.subscriptions.add(channel)
        finally:
            self.subscriptions_lock.release()

    def unsubscribe(self, channel):
        """Unsubscribe from the provided channel."""
        command = "UNSUBSCRIBE {channel}".format(channel=channel)
        command_msg = snakemq.message.Message(command.encode("utf-8"), ttl=60)
        self.messaging.send_message(self.broker_identity, command_msg)
        self.subscriptions_lock.acquire()
        try:
            self.subscriptions.remove(channel)
        finally:
            self.subscriptions_lock.release()
        logger.info("Unsubscribing {ident} from {channel}".format(
            ident=self.identity, channel=channel))

    def messenger_on_connect(self, conn, ident):
        """Resubscribe to any prior subscriptions."""
        super(Subscriber, self).messenger_on_connect(conn, ident)
        self.is_connected = True
        self.subscriptions_lock.acquire()
        try:
            for channel in self.subscriptions:
                self.subscribe(channel)
        finally:
            self.subscriptions_lock.release()

    def on_recv(self, conn, ident, message):
        """Forward a received message to the prior provided callback."""
        super(Subscriber, self).on_recv(conn, ident, message)
        self.on_recv_callback(conn, ident, message)

