import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as NodeContext from "@effect/platform-node/NodeContext"
import * as Runtime from "@effect/platform-node/Runtime"
import * as FS from "@effect/platform/FileSystem"
import * as S from "@effect/schema/Schema"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import pkg from "../package.json"

const Collector = S.union(S.literal("myaku/loc"))
const Frequency = S.union(S.literal("per-commit"))

const Config = S.struct({
  repository: S.string,
  metrics: S.record(
    S.string,
    S.struct({
      collector: Collector,
      frequency: Frequency,
    })
  ),
})

const loadConfig = (configPath: string) =>
  Effect.gen(function* ($) {
    const fs = yield* $(FS.FileSystem)
    const absoluteConfigPath = yield* $(fs.realPath(configPath))
    const configContent = yield* $(fs.readFileString(absoluteConfigPath))

    const parse = S.parseSync(S.fromJson(Config))
    const config = parse(configContent)

    return config
  })

const myakuCollect = Command.make(
  "collect",
  {
    config: Args.path({ name: "config" }),
  },
  ({ config: configPath }) =>
    Effect.gen(function* ($) {
      const config = yield* $(loadConfig(configPath))
      yield* $(Console.log("Loaded config:"))
      yield* $(Console.log(config))
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
