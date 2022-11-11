# Node.js Async FIFO/LIFO/PRIORITY Queue

Simple JavaScript async FIFO/LIFO/PRIORITY queue implementation.

## Overview

#### Parameters

- **maxsize** — Number of items allowed in the queue.
- **type** — The type of queue. Defaults to `'FIFO'`.
- **compareFn** — Comparison function for `PRIORITY` queues.

#### Queue Types

- **FIFO** — First in, first out.
- **LIFO** — Last in, first out.
- **PRIORITY** — Priority order.

#### Methods / Getters

- **async** - Get an object that allows async `get` and `put`.
- **get()** — Remove and get an item from the queue immediately.
- **put(item,priority)** — Put an item into the queue without blocking.
- **clear()** — Clear the queue and cancel all tasks.
- **join(timeout)** — Block until all items in the queue have been received and processed.
- **done()** — Indicate that a formerly enqueued task is complete.
- **empty()** — Check if the queue is empty.
- **full()** — Check if the queue is full (there are `maxsize` items in the queue).
- **count()** — Get the number of unfinished tasks in the queue.
- **size()** — Get the number of tasks in the queue.

## Examples

<details>
<summary><h5>FIFO Queue</h5></summary>

Retrieves least recently added entries first (first in, first out).

```js
const { setTimeout } = require('node:timers');
const { Queue } = require('./queue.js');

(async () => {
    const queue = new Queue();

    queue.put('item #1');
    await queue.async.put('item #2');

    const task = queue.get();
    console.log(task.value); // item #1
    task.done();

    setTimeout(async () => {
        const task = await queue.async.get();
        console.log(task.value); // item #2
        task.done(); // *1
    }, 1000);

    await queue.join(); // *1

    setTimeout(async () => {
        const task = await queue.async.get();
        console.log(task.done()); // item #3 (*2)
    });

    await queue.put('item #3').join(); // *2

    queue.async.get(1000).catch(console.log); // throws QueueTimeout
})();
```

```shell
item #1
item #2
item #3
QueueTimeout [Error]: Promise timed out after 1000 ms
```

</details>

<details>
<summary><h5>LIFO Queue</h5></summary>

Retrieves most recently added entries first (last in, first out).

```js
const { Queue } = require('./queue.js');

const queue = new Queue({ type: 'LIFO' });

queue.put('A');
queue.put('B');

for (const task of queue)
    console.log(task.done());
```

```shell
B
A
```

</details>

<details>
<summary><h5>PRIORITY Queue</h5></summary>

Retrieves tasks in priority order (default lowest first).
Option `compareFn` provides a user-defined comparison function.

```js
const { Queue } = require('./queue.js');

function compareFn(prev, curr)
{
    return prev < curr; // default (lowest first)
}

(async () => {
    const queue = new Queue({
        type: 'PRIORITY',
        compareFn
    });

    queue.put('C');
    queue.put('A');
    queue.put('B');

    for (const task of queue)
        console.log(task.done());

    console.log('-'.repeat(10));

    queue.put('C', 1);
    queue.put('A', 3);
    queue.put('B', 2);

    for (const task of queue)
        console.log(task.done());
})();
```

```shell
A
B
C
----------
C
B
A
```

</details>

## License

This project is licensed under the **GNU General Public License v3.0**. See the [license file](LICENSE) for details.
