import {rxBatch} from '../src/index';
import {mocked} from 'ts-jest/utils';
import {TestScheduler} from 'rxjs/testing';
import {of, throwError, Subject} from 'rxjs';
import {toArray} from 'rxjs/operators';

// Helper for RxJS 6 compatibility (lastValueFrom was added in RxJS 7)
function toPromise<T>(observable: import('rxjs').Observable<T>): Promise<T> {
    return observable.toPromise() as Promise<T>;
}

interface Data {
    id: string;
    name: string;
}

describe('rxBatch', () => {
    it('should buffer and batch', async () => {

        const testScheduler = new TestScheduler((actual, expected) => {
            expect(actual).toEqual(expected);
        });

        testScheduler.run(helpers => {
            const {cold, expectObservable, expectSubscriptions} = helpers;

            const requester = jest
                .fn()
                .mockImplementationOnce(() => {
                    return of([
                        {id: 'c', name: 'C'},
                        {id: 'a', name: 'A'},
                        {id: 'b', name: 'B'}
                    ]);
                })
                .mockImplementationOnce(() => {
                    return of([
                        {id: 'e', name: 'E'},
                        {id: 'd', name: 'D'}
                    ]);
                });

            const values = {
                a: {
                    args: 'a',
                    data: {
                        id: 'a',
                        name: 'A'
                    }
                },
                b: {
                    args: 'b',
                    data: {
                        id: 'b',
                        name: 'B'
                    }
                },
                c: {
                    args: 'c',
                    data: {
                        id: 'c',
                        name: 'C'
                    }
                },
                d: {
                    args: 'd',
                    data: {
                        id: 'd',
                        name: 'D'
                    }
                },
                e: {
                    args: 'e',
                    data: {
                        id: 'e',
                        name: 'E'
                    }
                }
            };

            const inputObs = cold('-abcd-e-----------|');
            const subs =          '^-----------------!';
            const expected =      '----------(abcde)-|';

            expectObservable(
                inputObs.pipe(
                    rxBatch({
                        request: mocked(requester),
                        bufferTime: 10,
                        batch: 3,
                        getArgs: (item: Data) => item.id,
                        getData: (item: Data) => item,
                        requestError: e => e,
                        missingError: () => 'missing'
                    })
                )
            ).toBe(expected, values);

            expectSubscriptions(inputObs.subscriptions).toBe(subs);
        });
    });

    it('should cope with errors', async () => {

        const testScheduler = new TestScheduler((actual, expected) => {
            expect(actual).toEqual(expected);
        });

        testScheduler.run(helpers => {
            const {cold, expectObservable, expectSubscriptions} = helpers;

            const requester = jest.fn(() => throwError('oops!'));

            const values = {
                a: {
                    args: 'a',
                    error: 'error: oops!'
                },
                b: {
                    args: 'b',
                    error: 'error: oops!'
                },
                c: {
                    args: 'c',
                    error: 'error: oops!'
                },
                d: {
                    args: 'd',
                    error: 'error: oops!'
                }
            };

            const inputObs = cold('-ab-------------cd-------|');
            const subs =          '^------------------------!';
            const expected =      '----------(ab)------(cd)-|';

            expectObservable(
                inputObs.pipe(
                    rxBatch({
                        request: mocked(requester),
                        bufferTime: 10,
                        batch: 3,
                        getArgs: (item: Data) => item.id,
                        getData: (item: Data) => item,
                        requestError: e => `error: ${e}`,
                        missingError: () => 'missing'
                    })
                )
            ).toBe(expected, values);

            expectSubscriptions(inputObs.subscriptions).toBe(subs);
        });
    });

    it('should return missing if item not found from request', async () => {

        const testScheduler = new TestScheduler((actual, expected) => {
            expect(actual).toEqual(expected);
        });

        testScheduler.run(helpers => {
            const {cold, expectObservable, expectSubscriptions} = helpers;

            const requester = jest
                .fn()
                .mockImplementationOnce(() => {
                    return of([
                        {id: 'a', name: 'A'},
                        {id: 'b', name: 'B'}
                    ]);
                });

            const values = {
                a: {
                    args: 'a',
                    data: {
                        id: 'a',
                        name: 'A'
                    }
                },
                b: {
                    args: 'b',
                    data: {
                        id: 'b',
                        name: 'B'
                    }
                },
                c: {
                    args: 'c',
                    error: 'missing'
                }
            };

            const inputObs = cold('-abc------------|');
            const subs =          '^---------------!';
            const expected =      '----------(abc)-|';

            expectObservable(
                inputObs.pipe(
                    rxBatch({
                        request: mocked(requester),
                        bufferTime: 10,
                        batch: 3,
                        getArgs: (item: Data) => item.id,
                        getData: (item: Data) => item,
                        requestError: e => e,
                        missingError: () => 'missing'
                    })
                )
            ).toBe(expected, values);

            expectSubscriptions(inputObs.subscriptions).toBe(subs);
        });
    });
});

describe('rxBatch parallel batching', () => {
    interface NumResult {
        args: number;
        result: number;
    }

    /**
     * Creates a mock request that tracks concurrent execution.
     * Uses a barrier pattern to ensure we can reliably measure concurrency.
     */
    const createMockRequestWithBarrier = (expectedBatches: number) => {
        let waitingCount = 0;
        let maxConcurrent = 0;
        let resolveBarrier: () => void;
        const barrierPromise = new Promise<void>(resolve => {
            resolveBarrier = resolve;
        });

        const request = jest.fn(async (argsArray: number[]): Promise<NumResult[]> => {
            waitingCount++;
            maxConcurrent = Math.max(maxConcurrent, waitingCount);

            // If this is the last expected batch, release the barrier
            if (waitingCount >= expectedBatches) {
                resolveBarrier();
            }

            // Wait on barrier
            await barrierPromise;
            waitingCount--;

            return argsArray.map(args => ({args, result: args * 2}));
        });

        return {request, getMaxConcurrent: () => maxConcurrent};
    };

    it('should process multiple batches in parallel up to maxConcurrency', async () => {
        // 10 items with batch size 2 = 5 batches
        const {request, getMaxConcurrent} = createMockRequestWithBarrier(5);
        const subject = new Subject<number>();

        const batchOperator = rxBatch<number, number, Error, NumResult>({
            request,
            bufferTime: 50,
            batch: 2,
            maxConcurrency: 5,
            getArgs: result => result.args,
            getData: result => result.result,
            requestError: (error) => error as Error,
            missingError: () => new Error('NOT_FOUND')
        });

        const resultsPromise = toPromise(subject.pipe(batchOperator, toArray()));

        // Emit 10 items - should create 5 batches of 2
        for (let i = 0; i < 10; i++) {
            subject.next(i);
        }
        subject.complete();

        const results = await resultsPromise;

        // Verify all results were received
        expect(results).toHaveLength(10);

        // Verify parallel execution occurred (all 5 batches should have been concurrent)
        expect(getMaxConcurrent()).toBe(5);
    });

    it('should respect maxConcurrency limit', async () => {
        const maxConcurrency = 3;
        // 10 items with batch size 1 = 10 batches, but only 3 concurrent
        const {request, getMaxConcurrent} = createMockRequestWithBarrier(maxConcurrency);
        const subject = new Subject<number>();

        const batchOperator = rxBatch<number, number, Error, NumResult>({
            request,
            bufferTime: 50,
            batch: 1,
            maxConcurrency,
            getArgs: result => result.args,
            getData: result => result.result,
            requestError: (error) => error as Error,
            missingError: () => new Error('NOT_FOUND')
        });

        const resultsPromise = toPromise(subject.pipe(batchOperator, toArray()));

        // Emit 10 items - should create 10 batches of 1
        for (let i = 0; i < 10; i++) {
            subject.next(i);
        }
        subject.complete();

        const results = await resultsPromise;

        // Verify all results were received
        expect(results).toHaveLength(10);

        // Verify maxConcurrency was respected (should be exactly 3)
        expect(getMaxConcurrent()).toBe(maxConcurrency);
    });

    it('should default maxConcurrency to 10 when not specified', async () => {
        // 15 items with batch size 1 = 15 batches, default max concurrent = 10
        const {request, getMaxConcurrent} = createMockRequestWithBarrier(10);
        const subject = new Subject<number>();

        const batchOperator = rxBatch<number, number, Error, NumResult>({
            request,
            bufferTime: 50,
            batch: 1,
            // maxConcurrency not specified - should default to 10
            getArgs: result => result.args,
            getData: result => result.result,
            requestError: (error) => error as Error,
            missingError: () => new Error('NOT_FOUND')
        });

        const resultsPromise = toPromise(subject.pipe(batchOperator, toArray()));

        // Emit 15 items
        for (let i = 0; i < 15; i++) {
            subject.next(i);
        }
        subject.complete();

        const results = await resultsPromise;

        expect(results).toHaveLength(15);
        // Should have parallel execution with default limit of 10
        expect(getMaxConcurrent()).toBe(10);
    });

    it('should handle errors correctly with parallel batches', async () => {
        const errorRequest = jest.fn(async (argsArray: number[]): Promise<NumResult[]> => {
            // Fail for specific args
            if (argsArray.includes(5)) {
                throw new Error('Batch 5 failed');
            }
            return argsArray.map(args => ({args, result: args * 2}));
        });

        const subject = new Subject<number>();

        const batchOperator = rxBatch<number, number, Error, NumResult>({
            request: errorRequest,
            bufferTime: 50,
            batch: 1,
            maxConcurrency: 5,
            getArgs: result => result.args,
            getData: result => result.result,
            requestError: (error) => error as Error,
            missingError: () => new Error('NOT_FOUND')
        });

        const resultsPromise = toPromise(subject.pipe(batchOperator, toArray()));

        // Emit items including one that will fail
        for (let i = 0; i < 10; i++) {
            subject.next(i);
        }
        subject.complete();

        const results = await resultsPromise;

        // All items should have results (either data or error)
        expect(results).toHaveLength(10);

        // The failed batch should have error
        type ResultItem = {args: number; data?: number; error?: Error};
        const errorResult = (results as ResultItem[]).find((r: ResultItem) => 'error' in r && r.error);
        expect(errorResult).toBeDefined();
        expect(errorResult?.error?.message).toBe('Batch 5 failed');

        // Successful results should have data
        const successResults = (results as ResultItem[]).filter((r: ResultItem) => 'data' in r && r.data !== undefined);
        expect(successResults.length).toBe(9);
    });
});
