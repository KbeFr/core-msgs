"""
dispatch.py

For topic callback register is both aggregate and instance

"""
from __future__ import annotations



def handles(topic):
    """Tag a method as the handler for a MessageType."""

    def deco(fn):
        # Tag topic attr to decorated function itself
        fn._topic = topic
        return fn
    return deco


class MessageDispatcher:
    """Base class that automatically compiles the dispatch table for any subclass."""

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        table = {}
        # method traversal in class attached
        for klass in reversed(cls.__mro__):
            for name, fn in vars(klass).items():
                # check if metadata above attached to this function
                topic = getattr(fn, "_topic", None)
                if topic is None:
                    continue
                if topic in table and table[topic] != name:
                    raise TypeError(f"{cls.__name__}: {topic} claimed by {table[topic]}() and {name}()")
                # store function name to topic
                table[topic] = name
        # store the table as private field of class
        cls._DISPATCH = table

    def dispatch(self, topic, *args, **kwargs):
        handler_name = getattr(self, "_DISPATCH", {}).get(topic)
        if not handler_name:
            raise NotImplementedError(f"No handler registered for {topic}")
        return getattr(self, handler_name)(*args, **kwargs)





