import {
  type EnvironmentId as EnvironmentIdType,
  type SourceControlCloneProgressEvent,
  type SourceControlCloneRepositoryInput,
  type SourceControlCloneRepositoryResult,
  WS_METHODS,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { Atom } from "effect/unstable/reactivity";

import {
  createAtomCommandScheduler,
  createEnvironmentRpcCommand,
  createEnvironmentRpcQueryAtomFamily,
  createRuntimeCommand,
  runStreamInEnvironment,
} from "./runtime.ts";
import type { EnvironmentRegistry } from "../connection/registry.ts";
import { EnvironmentCacheStore } from "../platform/persistence.ts";
import { runStream } from "../rpc/client.ts";
import { vcsCommandConcurrency, vcsCommandScheduler } from "./vcsCommandScheduler.ts";
import { invalidateCachedVcsRefs } from "./vcsRefInvalidation.ts";

export interface CloneRepositoryWithProgressInput {
  readonly environmentId: EnvironmentIdType;
  readonly input: SourceControlCloneRepositoryInput;
  readonly onProgress?: (event: SourceControlCloneProgressEvent) => void;
}

export function createSourceControlEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | EnvironmentCacheStore | R, E>,
) {
  const commandScheduler = createAtomCommandScheduler();
  return {
    discovery: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:server:source-control-discovery",
      tag: WS_METHODS.serverDiscoverSourceControl,
    }),
    repository: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:source-control:repository",
      tag: WS_METHODS.sourceControlLookupRepository,
    }),
    cloneRepository: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:source-control:clone-repository",
      tag: WS_METHODS.sourceControlCloneRepository,
      scheduler: commandScheduler,
      concurrency: {
        mode: "serial",
        key: ({ environmentId }) => environmentId,
      },
    }),
    cloneRepositoryWithProgress: createRuntimeCommand(runtime, {
      label: "environment-data:source-control:clone-repository-with-progress",
      scheduler: commandScheduler,
      concurrency: { mode: "serial", key: (input) => input.environmentId },
      execute: (
        input: CloneRepositoryWithProgressInput,
      ): Effect.Effect<
        SourceControlCloneRepositoryResult,
        unknown,
        EnvironmentRegistry | EnvironmentCacheStore | R
      > =>
        Effect.gen(function* () {
          let result: SourceControlCloneRepositoryResult | null = null;
          yield* runStreamInEnvironment(
            input.environmentId,
            runStream(WS_METHODS.sourceControlCloneRepositoryWithProgress, input.input),
          ).pipe(
            Stream.runForEach((event) =>
              Effect.sync(() => {
                if (event.kind === "finished") {
                  result = event.result;
                }
                input.onProgress?.(event);
              }),
            ),
          );
          if (result === null) {
            throw new Error("Clone progress stream ended without a result.");
          }
          return result;
        }),
    }),
    publishRepository: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:source-control:publish-repository",
      tag: WS_METHODS.sourceControlPublishRepository,
      scheduler: vcsCommandScheduler,
      concurrency: vcsCommandConcurrency,
      onSettled: (target, registry) =>
        invalidateCachedVcsRefs(registry, {
          environmentId: target.environmentId,
          cwd: target.input.cwd,
        }),
    }),
  };
}
