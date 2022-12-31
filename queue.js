'use strict';

const util = require('node:util');
const { setTimeout, clearTimeout } = require('node:timers');

/**
 * Removes elements from an array.
 */
function remove(array, value, count=1)
{
    const index = array.indexOf(value);
    if (index !== -1)
        return array.splice(index, count);
}

/**
 * Comparison function for `PRIORITY` queues.
 */
function priorityFn(previous, current, index)
{
    if (!previous) return { item: current, index };
    const prev = previous.item.priority ?? previous.item.value;
    const curr = current.priority ?? current.value;
    const result = typeof(this._compareFn) === 'function'
        ? this._compareFn.call(this, prev, curr)
        : prev < curr; // default (lowest first)
    return result ? previous : { item: current, index };
}

/**
 * Base class for {@link Queue} exceptions.
 */
class QueueError extends Error {
    constructor(message, ...args) {
        super(util.format(message, ...args.map(x => util.inspect(x))));
    }
}

/**
 * Exception thrown when {@link AsyncManager.get()} or {@link AsyncManager.set()} times out.
 */
class QueueTimeout extends QueueError {
    constructor(timeout) {
        super('Promise timed out after %s ms', timeout);
    }
}

/**
 * Exception thrown when {@link AsyncManager.get()} or {@link AsyncManager.put()} has been canceled.
 */
class QueueCanceled extends QueueError { }

/**
 * Exception thrown when {@link Queue.get()} is called on an empty queue.
 */
class QueueEmpty extends QueueError { }

/**
 * Exception thrown when {@link Queue.put()} is called on a full queue.
 */
class QueueFull extends QueueError { }

/**
 * Manages an asynchronous operation for a specific task.
 */
class AsyncManager
{
    /**
     * Create an AsyncManager object.
     * @param {Queue} queue The queue associated with this object.
     */
    constructor(queue)
    {
        this.queue = queue;
    }

    /**
     * Remove and get an item from the queue.
     * If queue is empty, wait until an item is available.
     * @param {Number?} timeout Timeout, in milliseconds.
     * @return {Promise<QueueTask>} Returns a {@link QueueTask} object.
     * @remarks
     * - The value associated with the task is stored in `QueueTask.value`.
     * - The {@link QueueTask.done()} method should be called once the task has finished processing.
     * - Throws {@link QueueTimeout} if the timeout has expired.
     * - Throws {@link QueueCanceled} if the operation or task has been canceled.
     */
    async get(timeout)
    {
        if (this._resolve)
            throw new QueueError('There is already an ongoing operation');
        while (this.queue.empty())
            await this.#wait(this.queue._getters, timeout);
        return this.queue.get();
    }

    /**
     * Put an item into the queue.
     * If the queue is full, wait until a free slot is available before adding the item.
     * @param item The item to be added.
     * @param {Number?} priority Task priority. Only valid for `PRIORITY` queues.
     * @param {Number?} timeout Timeout, in milliseconds.
     * @return {Promise<QueueTask>} Returns a {@link QueueTask} object.
     * @remarks
     * - Throws {@link QueueTimeout} if the timeout has expired.
     * - Throws {@link QueueCanceled} if the operation has been canceled.
     */
    async put(item, priority, timeout)
    {
        if (this._resolve)
            throw new QueueError('There is already an ongoing operation');
        while (this.queue.full())
            await this.#wait(this.queue._putters, timeout);
        return this.queue.put(item, priority);
    }

    /**
     * Cancel {@link get} or {@link put}.
     */
    cancel()
    {
        if (!this._resolve)
            throw new QueueError('There are no ongoing operations');
        this._resolve(new QueueCanceled());
        remove(this._array, this._resolve);
    }

    async #wait(arr, timeout)
    {
        let timer;
        const result = await new Promise(resolve => {
            if (timeout) timer = setTimeout(() => {
                resolve(new QueueTimeout(timeout));
                remove(arr, resolve);
            }, timeout);
            (this._array = arr).push(this._resolve = resolve);
        });
        clearTimeout(timer);
        remove(arr, this._resolve);
        delete this._resolve;
        delete this._array;
        if (result instanceof Error)
            throw result;
        return result;
    }
}

/**
 * Represents a task.
 * - This object is instantiated when an item is put in the queue.
 * - An instance of this object is returned when an item is retrieved from the queue.
 */
class QueueTask
{
    /**
     * Create a QueueTask object.
     * @param {Queue} queue The queue associated with the task.
     * @param item Task value.
     * @param priority Task priority. Only valid for `PRIORITY` queues.
     */
    constructor(queue, item, priority)
    {
        this.queue = queue;
        this.value = item;
        this.priority = priority;
        this._promise = new Promise(resolve => {
            this._resolve = resolve;
        });
    }

    /**
     * Block until the task has been completed.
     */
    async join()
    {
        return await this._promise;
    }

    /**
     * Indicates that this task has been completed.
     * @param value Any value with which to resolve the promise.
     * @return Returns the task value.
     */
    done(value)
    {
        if (!this._resolve)
            throw new QueueError('The task has already been completed');
        this._resolve(value);
        delete this._resolve;
        this.queue.done();
        return this.value;
    }

    /**
     * Returns a string representation of the value associated with the task.
     */
    toString()
    {
        return `${this.value}`;
    }
}

/**
 * Simple async FIFO/LIFO queue implementation.
 */
class Queue
{
    _items = []; // Array of tasks.
    _getters = []; // Array of promises awaiting for a task to be added.
    _putters = []; // Array of promises awaiting for a task to be removed.
    _joiners = []; // Array of promises awaiting completion of all tasks.
    _counter = 0; // Number of (retrieved) unfinished tasks.

    /**
     * Create a Queue object.
     * @param {Object} options Options.
     * @param {Number} options.maxsize
     * Number of items allowed in the queue.
     * If `maxsize` is less than or equal to zero, the queue size is infinite.
     * If it is an integer greater than zero, then `async.put()` blocks when the queue reaches `maxsize` until an item is removed.
     * @param {'FIFO'|'LIFO'|'PRIORITY'} options.type
     * The type of queue. Defaults to `'FIFO'`.
     * - `'FIFO'` - Retrieves least recently added entries first (first in, first out).
     * - `'LIFO'` - Retrieves most recently added entries first (last in, first out).
     * - `'PRIORITY'` - Retrieves tasks in priority order (lowest first).
     * @param {Function} options.compareFn Comparison function for `PRIORITY` queues.
     */
    constructor(options)
    {
        this._maxsize = options?.maxsize;
        this._type = options?.type ?? 'FIFO';
        this._compareFn = options?.compareFn;
    }

    /**
     * Get an {@link AsyncManager} object that allows asynchronous operations for a task.
     */
    get async()
    {
        return new AsyncManager(this);
    }

    /**
     * Remove and get an item from the queue immediately.
     * @return {QueueTask} Returns a {@link QueueTask} object.
     * @remarks
     * - The value associated with the task is stored in `QueueTask.value`.
     * - The {@link QueueTask.done()} method should be called once the task has finished processing.
     * - Throws {@link QueueEmpty} if the queue is empty.
     */
    get()
    {
        if (this.empty())
            throw new QueueEmpty();
        this._putters.shift()?.();
        ++this._counter;
        switch (this._type)
        {
            case 'FIFO':
                return this._items.shift();
            case 'LIFO':
                return this._items.pop();
            case 'PRIORITY':
                const result = this._items.reduce(priorityFn.bind(this), null);
                return this._items.splice(result.index, 1)[0];
            default: // this should not (and must not) ever happen
                throw new QueueError(`[INSTABILITY] Invalid queue type: ${this._type}`);
        }
    }

    /**
     * Put an item into the queue without blocking.
     * @param item The item to be added.
     * @param priority Task priority. Defaults to `item`.
     * @return {QueueTask} Returns a {@link QueueTask} object.
     * @remarks Throws {@link QueueFull} if the queue is full.
     */
    put(item, priority)
    {
        if (this.full())
            throw new QueueFull();
        const task = new QueueTask(this, item, priority);
        this._items.push(task);
        this._getters.shift()?.();
        return task;
    }

    /**
     * Clear the queue and cancel all tasks.
     * @returns {QueueTask[]} List of canceled tasks.
     */
    clear()
    {
        const error = new QueueCanceled('The queue has been cleared');
        while (this._getters.length)
            this._getters.shift()(error);
        while (this._putters.length)
            this._putters.shift()(error);
        for (const task of this._items)
            ++this._counter, task.done(error);
        return this._items.splice(0);
    }

    /**
     * Block until all items in the queue have been received and processed.
     * @param {Number?} timeout Timeout, in milliseconds.
     * @remarks
     * - The count of unfinished tasks increases each time one is added to the queue.
     * - The count of unfinished tasks decreases each time {@link done()} is called to indicate that an item was retrieved and all work on it is completed.
     * - When the count of unfinished tasks drops to zero, {@link join()} unblocks.
     * - Throws {@link QueueTimeout} if the timeout has expired.
     */
    async join(timeout)
    {
        if (this.size() || this.count())
        {
            let timer;
            const result = await new Promise(resolve => {
                if (timeout) timer = setTimeout(() => {
                    resolve(new QueueTimeout(timeout));
                }, timeout);
                this._joiners.push(resolve);
            });
            clearTimeout(timer);
            if (result instanceof Error)
                throw result;
        }
    }

    /**
     * Indicate that a formerly enqueued task is complete.
     * @remarks
     * - For each retrieved task, a subsequent call to this function tells the queue that the processing on the task is complete.
     * - All blocking {@link join()}s will resume when all items have been processed (meaning that a {@link done()} call was received for every item that had been put into the queue).
     * - Throws {@link QueueError} if called more times than there were items placed in the queue.
     */
    done()
    {
        if (this.count() < 1)
            throw new QueueError('There are no retrieved tasks to mark as completed');
        --this._counter;
        if (this.empty() && !this.count())
            while (this._joiners.length)
                this._joiners.shift()();
    }

    /**
     * Check if the queue is empty.
     */
    empty()
    {
        return this._items.length === 0;
    }

    /**
     * Check if the queue is full (there are `maxsize` items in the queue).
     * If the queue was initialized with `maxsize=0`, then `full()` never returns `true`.
     */
    full()
    {
        return this._items.length === this._maxsize;
    }

    /**
     * Get the number of unfinished tasks in the queue.
     */
    count()
    {
        return this._counter;
    }

    /**
     * Get the number of tasks in the queue.
     */
    size()
    {
        return this._items.length;
    }

    /**
     * Iterate through all queued tasks.
     */
    *[Symbol.iterator]()
    {
        while (!this.empty())
            yield this.get();
    }

    /**
     * Asynchronously iterate through all queued tasks.
     */
    async *[Symbol.asyncIterator]()
    {
        while (!this.empty())
            yield Promise.resolve(this.get());
    }
}

module.exports = {
    QueueError,
    QueueEmpty,
    QueueFull,
    QueueTimeout,
    QueueCanceled,
    AsyncManager,
    QueueTask,
    Queue
};
