"""Event loop and event loop policy."""

__all__ = [
           'AbstractEventLoop',
           'Handle', 'TimerHandle',
           ]

import functools
import reprlib


def _format_args_and_kwargs(args, kwargs):
    """Format function arguments and keyword arguments.

    Special case for a single parameter: ('hello',) is formatted as ('hello').
    """
    # use reprlib to limit the length of the output
    items = []
    if args:
        items.extend(reprlib.repr(arg) for arg in args)
    if kwargs:
        items.extend('{}={}'.format(k, reprlib.repr(v))
                     for k, v in kwargs.items())
    return '(' + ', '.join(items) + ')'


def _format_callback(func, args, kwargs, suffix=''):
    if isinstance(func, functools.partial):
        suffix = _format_args_and_kwargs(args, kwargs) + suffix
        return _format_callback(func.func, func.args, func.keywords, suffix)

    if hasattr(func, '__qualname__'):
        func_repr = getattr(func, '__qualname__')
    elif hasattr(func, '__name__'):
        func_repr = getattr(func, '__name__')
    else:
        func_repr = repr(func)

    func_repr += _format_args_and_kwargs(args, kwargs)
    if suffix:
        func_repr += suffix
    return func_repr

def _format_callback_source(func, args):
    func_repr = _format_callback(func, args, None)
    return func_repr


class Handle:
    """Object returned by callback registration methods."""

    __slots__ = ('_callback', '_args', '_cancelled', '_loop',
                 '_source_traceback', '_repr', '__weakref__')

    def __init__(self, callback, args, loop):
        self._loop = loop
        self._callback = callback
        self._args = args
        self._cancelled = False
        self._repr = None
        self._source_traceback = None

    def _repr_info(self):
        info = [self.__class__.__name__]
        if self._cancelled:
            info.append('cancelled')
        if self._callback is not None:
            info.append(_format_callback_source(self._callback, self._args))
        if self._source_traceback:
            frame = self._source_traceback[-1]
            info.append('created at %s:%s' % (frame[0], frame[1]))
        return info

    def __repr__(self):
        if self._repr is not None:
            return self._repr
        info = self._repr_info()
        return '<%s>' % ' '.join(info)

    def cancel(self):
        if not self._cancelled:
            self._cancelled = True
            self._callback = None
            self._args = None

    def _run(self):
        try:
            self._callback(*self._args)
        except Exception as exc:
            cb = _format_callback_source(self._callback, self._args)
            msg = 'Exception in callback {}'.format(cb)
            context = {
                'message': msg,
                'exception': exc,
                'handle': self,
            }
            if self._source_traceback:
                context['source_traceback'] = self._source_traceback
            self._loop.call_exception_handler(context)
        self = None  # Needed to break cycles when an exception occurs.


class TimerHandle(Handle):
    """Object returned by timed callback registration methods."""

    __slots__ = ['_scheduled', '_when']

    def __init__(self, when, callback, args, loop):
        assert when is not None
        super().__init__(callback, args, loop)
        if self._source_traceback:
            del self._source_traceback[-1]
        self._when = when
        self._scheduled = False

    def _repr_info(self):
        info = super()._repr_info()
        pos = 2 if self._cancelled else 1
        info.insert(pos, 'when=%s' % self._when)
        return info

    def __hash__(self):
        return hash(self._when)

    def __lt__(self, other):
        return self._when < other._when

    def __le__(self, other):
        if self._when < other._when:
            return True
        return self.__eq__(other)

    def __gt__(self, other):
        return self._when > other._when

    def __ge__(self, other):
        if self._when > other._when:
            return True
        return self.__eq__(other)

    def __eq__(self, other):
        if isinstance(other, TimerHandle):
            return (self._when == other._when and
                    self._callback == other._callback and
                    self._args == other._args and
                    self._cancelled == other._cancelled)
        return NotImplemented

    def __ne__(self, other):
        equal = self.__eq__(other)
        return NotImplemented if equal is NotImplemented else not equal

    def cancel(self):
        if not self._cancelled:
            self._loop._timer_handle_cancelled(self)
        super().cancel()


class AbstractEventLoop:
    """Abstract event loop."""

    # Running and stopping the event loop.

    def run_forever(self):
        """Run the event loop until stop() is called."""
        raise NotImplementedError

    def run_until_complete(self, future):
        """Run the event loop until a Future is done.

        Return the Future's result, or raise its exception.
        """
        raise NotImplementedError

    def stop(self):
        """Stop the event loop as soon as reasonable.

        Exactly how soon that is may depend on the implementation, but
        no more I/O callbacks should be scheduled.
        """
        raise NotImplementedError

    def is_running(self):
        """Return whether the event loop is currently running."""
        raise NotImplementedError

    def is_closed(self):
        """Returns True if the event loop was closed."""
        raise NotImplementedError

    def close(self):
        """Close the loop.

        The loop should not be running.

        This is idempotent and irreversible.

        No other methods should be called after this one.
        """
        raise NotImplementedError

    # Methods scheduling callbacks.  All these return Handles.

    def _timer_handle_cancelled(self, handle):
        """Notification that a TimerHandle has been cancelled."""
        raise NotImplementedError

    def call_soon(self, callback, *args):
        return self.call_later(0, callback, *args)

    def call_later(self, delay, callback, *args):
        raise NotImplementedError

    def call_at(self, when, callback, *args):
        raise NotImplementedError

    def time(self):
        raise NotImplementedError

    def create_future(self):
        raise NotImplementedError

    # Method scheduling a coroutine object: create a task.

    def create_task(self, coro):
        raise NotImplementedError

    # Methods for interacting with threads.

    def call_soon_threadsafe(self, callback, *args):
        raise NotImplementedError

    # Task factory.

    def set_task_factory(self, factory):
        raise NotImplementedError

    def get_task_factory(self):
        raise NotImplementedError

    # Error handlers.

    def get_exception_handler(self):
        raise NotImplementedError

    def set_exception_handler(self, handler):
        raise NotImplementedError

    def default_exception_handler(self, context):
        raise NotImplementedError

    def call_exception_handler(self, context):
        raise NotImplementedError
