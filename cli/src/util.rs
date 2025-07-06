use regex::Regex;

pub fn get_repository_name_from_url(url: &str) -> Option<String> {
    let re =
        Regex::new(r"((git|ssh|http(s)?)|(git@[\w\.]+))(:(//)?)(?<main>[\w\.@\:/\-~]+)(\.git)(/)?")
            .expect("Failed to compile git repository url regex");
    re.captures(url).map(|caps| caps["main"].to_string())
}
