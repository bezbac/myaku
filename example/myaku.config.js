export default {
  repository: "git@github.com:RockstarLang/rockstar.git",
  metrics: [
    {
      name: "loc",
      collector: "myaku/loc",
      frequency: "per-commit",
    },
  ],
}
