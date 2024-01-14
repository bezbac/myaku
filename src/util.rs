use regex::Regex;

pub fn get_repository_name_from_url(url: &str) -> String {
    let re =
        Regex::new(r"((git|ssh|http(s)?)|(git@[\w\.]+))(:(//)?)(?<main>[\w\.@\:/\-~]+)(\.git)(/)?")
            .unwrap();
    let caps = re.captures(url).unwrap();
    caps["main"].to_string()
}
