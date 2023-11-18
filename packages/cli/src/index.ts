import * as Args from "@effect/cli/Args"
import * as Cli from "@effect/cli/CliApp"
import * as Command from "@effect/cli/Command"
import * as HelpDoc from "@effect/cli/HelpDoc"
import * as Options from "@effect/cli/Options"
import * as Node from "@effect/platform-node/Runtime"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import { pipe } from "effect/Function"
import * as Option from "effect/Option"
import pkg from "../package.json"

export interface Collect extends Data.Case {
  readonly _tag: "Collect"
  readonly directory: string
}

export const Collect = Data.tagged<Collect>("Collect")

export interface Myaku extends Data.Case {
  readonly version: boolean
  readonly subcommand: Option.Option<Collect>
}

export const Myaku = Data.case<Myaku>()

const collect: Command.Command<Collect> = pipe(
  Command.make("collect", {
    args: Args.text({ name: "directory" }),
  }),
  Command.withHelp(HelpDoc.p("Description of the `mayku collect` subcommand")),
  Command.map(({ args: directory }) => Collect({ directory }))
)

const myaku: Command.Command<Myaku> = pipe(
  Command.make("myaku", {
    options: Options.boolean("version").pipe(Options.alias("v")),
  }),
  Command.subcommands([collect]),
  Command.map(({ options: version, subcommand }) =>
    Myaku({ version, subcommand })
  )
)

const handleMyakuSubcommand = (
  command: Collect
): Effect.Effect<never, never, void> => {
  switch (command._tag) {
    case "Collect": {
      const msg = `Executing 'myaku collect ${command.directory}'`
      return Effect.log(msg)
    }
  }
}

const cli = Cli.make({
  name: "Myaku CLI",
  version: pkg.version,
  command: myaku,
})

const main = Cli.run(cli, process.argv.slice(2), (command) =>
  Option.match(command.subcommand, {
    onNone: () =>
      command.version ? Effect.log(`Executing 'myaku --version'`) : Effect.unit,
    onSome: handleMyakuSubcommand,
  })
)

Node.runMain(main.pipe(Effect.tapErrorCause(Effect.logError)))
