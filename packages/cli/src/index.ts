import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as NodeContext from "@effect/platform-node/NodeContext"
import * as Runtime from "@effect/platform-node/Runtime"
import * as FS from "@effect/platform/FileSystem"
import * as Path from "@effect/platform/Path"
import * as S from "@effect/schema/Schema"
import { pipe } from "effect"
import { UnknownException } from "effect/Cause"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import simpleGit, { SimpleGit } from "simple-git"
import pkg from "../package.json"

const Collector = S.union(S.literal("myaku/loc"))
const Frequency = S.union(S.literal("per-commit"))

const Config = S.struct({
  reference: S.struct({
    url: S.string,
    branch: S.string,
  }),
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

const getRepositoryNameFromUrl = (repositoryUrl: string): string =>
  pipe(
    repositoryUrl,
    (url) => (url.endsWith("/") ? url.slice(0, -1) : url),
    (url) => url.split("/"),
    (parts) => parts[parts.length - 1],
    (x) => x.slice(0, -4)
  )

if (import.meta.vitest) {
  const { expect, test, describe } = import.meta.vitest

  describe("getRepositoryNameFromUrl", () => {
    test.each([
      ["https://github.com/user/repo.git", "repo"],
      ["git@github.com:user/repo.git", "repo"],
    ] as const)("getRepositoryNameFromUrl(%s) -> %s", (url, expected) => {
      expect(getRepositoryNameFromUrl(url)).toBe(expected)
    })
  })
}

const clone = (git: SimpleGit, repoPath: string, localPath: string = ".") =>
  Effect.gen(function* ($) {
    yield* $(Effect.tryPromise(() => git.clone(repoPath, localPath)))
  }) as Effect.Effect<NodeContext.NodeContext, UnknownException, void>

const checkout = (git: SimpleGit, branch: string) =>
  Effect.gen(function* ($) {
    yield* $(Effect.tryPromise(() => git.checkout(branch)))
  }) as Effect.Effect<NodeContext.NodeContext, UnknownException, void>

const pull = (git: SimpleGit) =>
  Effect.gen(function* ($) {
    yield* $(Effect.tryPromise(() => git.pull()))
  }) as Effect.Effect<NodeContext.NodeContext, UnknownException, void>

const prepareGitRepo = ({
  url: repositoryUrl,
  branch,
}: S.Schema.To<typeof Config>["reference"]) =>
  Effect.gen(function* ($) {
    const fs = yield* $(FS.FileSystem)
    const path = yield* $(Path.Path)

    const repositoryName = getRepositoryNameFromUrl(repositoryUrl)
    const repositoryTempdirName = repositoryUrl.replace(/[^a-zA-Z0-9]/g, "_")

    const tmpdir = yield* $(fs.makeTempDirectory())
    const repoDir = path.join(tmpdir, "myaku", repositoryTempdirName)

    yield* $(fs.makeDirectory(repoDir, { recursive: true }))

    const gitDirectoryExists = yield* $(fs.exists(path.join(repoDir, ".git")))

    const git: SimpleGit = simpleGit({
      baseDir: repoDir,
      binary: "git",
      maxConcurrentProcesses: 6,
      trimmed: false,
    })

    if (gitDirectoryExists) {
      yield* $(Console.log("Git repository already exists, skipping clone..."))
      yield* $(checkout(git, branch))
      yield* $(pull(git))
    } else {
      yield* $(
        Console.log(`Cloning repository ${repositoryName} into ${repoDir}`)
      )
      yield* $(clone(git, repositoryUrl))
      yield* $(Console.log(`Successfully cloned repository`))
      yield* $(checkout(git, branch))
    }
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
      yield* $(prepareGitRepo(config.reference))
    })
)

const myaku = Command.make("myaku", {})

const command = myaku.pipe(Command.withSubcommands([myakuCollect]))

const cli = Command.run(command, {
  name: "Myaku CLI",
  version: pkg.version,
})

if (!import.meta.vitest) {
  Effect.suspend(() => cli(process.argv.slice(2))).pipe(
    Effect.provide(NodeContext.layer),
    Effect.tapErrorCause(Effect.logError),
    Runtime.runMain
  )
}
