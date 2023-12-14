import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as NodeContext from "@effect/platform-node/NodeContext"
import * as Runtime from "@effect/platform-node/Runtime"
import * as FS from "@effect/platform/FileSystem"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import pkg from "../package.json"

const myakuCollect = Command.make(
  "collect",
  {
    config: Args.path({ name: "config" }),
  },
  ({ config }) =>
    Effect.gen(function* ($) {
      const fs = yield* $(FS.FileSystem)
      const doesConfigExist = yield* $(fs.exists(config))

      if (!doesConfigExist) {
        yield* $(Console.error("Config file does not exist"))
        return
      }

      const absoluteConfigPath = yield* $(fs.realPath(config))

      yield* $(
        Console.debug("Continuing with config file: " + absoluteConfigPath)
      )
    })
)

const myaku = Command.make("myaku", {})

const command = myaku.pipe(Command.withSubcommands([myakuCollect]))

const cli = Command.run(command, {
  name: "Myaku CLI",
  version: pkg.version,
})

Effect.suspend(() => cli(process.argv.slice(2))).pipe(
  Effect.provide(NodeContext.layer),
  Effect.tapErrorCause(Effect.logError),
  Runtime.runMain
)
