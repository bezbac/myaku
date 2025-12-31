use regex::Regex;

pub fn get_repository_name_from_url(url: &str) -> Option<String> {
    if url.contains("://github.com") {
        let parts: Vec<&str> = url.split('/').collect();
        if parts.len() >= 5 {
            let user = parts[3];
            let repo = parts[4].trim_end_matches(".git");
            return Some(format!("{user}/{repo}"));
        }
        return None;
    }

    let re =
        Regex::new(r"((git|ssh|http(s)?)|(git@[\w\.]+))(:(//)?)(?<main>[\w\.@\:/\-~]+)(\.git)(/)?")
            .expect("Failed to compile git repository url regex");
    re.captures(url).map(|caps| caps["main"].to_string())
}

mod test {
    #[test]
    fn test_get_repository_name_from_url_case_https_github_com_user_repo() {
        let result = super::get_repository_name_from_url("https://github.com/user/repo");
        let expected = Some("user/repo".to_string());
        assert_eq!(&result, &expected);
    }

    #[test]
    fn test_get_repository_name_from_url_case_http_github_com_user_repo() {
        let result = super::get_repository_name_from_url("http://github.com/user/repo");
        let expected = Some("user/repo".to_string());
        assert_eq!(&result, &expected);
    }

    #[test]
    fn test_get_repository_name_from_url_case_git_at_github_com_user_repo() {
        let result = super::get_repository_name_from_url("git@github.com:user/repo.git");
        let expected = Some("user/repo".to_string());
        assert_eq!(&result, &expected);
    }
}
