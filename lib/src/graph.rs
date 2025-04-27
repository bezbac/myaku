use std::collections::HashMap;

use anyhow::Result;
use chrono::{Datelike, Timelike};
use petgraph::{graph::NodeIndex, Graph};

use crate::{
    config::{CollectorConfig, MetricConfig},
    git::{CommitHash, CommitInfo},
    Frequency,
};

#[derive(PartialEq, Clone, Debug)]
pub struct CollectionTask {
    pub commit_hash: CommitHash,
    pub collector_config: CollectorConfig,
}

#[derive(Clone, Debug)]
pub struct CollectionGraphEdge {
    /// The number of commits between the two nodes
    pub distance: usize,
}

#[derive(Clone, Debug)]
pub struct CollectionExecutionGraph {
    pub graph: Graph<CollectionTask, CollectionGraphEdge>,
}

pub fn add_task(
    graph: &mut Graph<CollectionTask, CollectionGraphEdge>,
    created_tasks: &mut HashMap<(CollectorConfig, CommitHash), NodeIndex>,
    collector_config: &CollectorConfig,
    current_commit_hash: &CommitHash,
    previous_commit_hash: Option<&CommitHash>,
    previous_commit_distance: usize,
) -> Result<NodeIndex> {
    if let Some(node_idx) =
        created_tasks.get(&(collector_config.clone(), current_commit_hash.clone()))
    {
        return Ok(*node_idx);
    }

    let node_idx = graph.add_node(CollectionTask {
        commit_hash: current_commit_hash.clone(),
        collector_config: collector_config.clone(),
    });

    created_tasks.insert(
        (collector_config.clone(), current_commit_hash.clone()),
        node_idx,
    );

    // Create dependency tasks
    match &collector_config {
        CollectorConfig::TotalPatternOccurences { pattern } => {
            let dependency_node_idx = add_task(
                graph,
                created_tasks,
                &CollectorConfig::PatternOccurences {
                    pattern: pattern.clone(),
                },
                current_commit_hash,
                previous_commit_hash,
                previous_commit_distance,
            )?;

            graph.add_edge(
                dependency_node_idx,
                node_idx,
                CollectionGraphEdge { distance: 0 },
            );
        }
        CollectorConfig::PatternOccurences { pattern: _ } | CollectorConfig::TotalCargoDeps => {
            let dependency_node_idx = add_task(
                graph,
                created_tasks,
                &CollectorConfig::ChangedFiles,
                current_commit_hash,
                previous_commit_hash,
                previous_commit_distance,
            )?;

            graph.add_edge(
                dependency_node_idx,
                node_idx,
                CollectionGraphEdge { distance: 0 },
            );
        }
        CollectorConfig::TotalLoc => {
            let dependency_node_idx = add_task(
                graph,
                created_tasks,
                &CollectorConfig::Loc,
                current_commit_hash,
                previous_commit_hash,
                previous_commit_distance,
            )?;

            graph.add_edge(
                dependency_node_idx,
                node_idx,
                CollectionGraphEdge { distance: 0 },
            );
        }
        CollectorConfig::TotalFileCount => {
            let dependency_node_idx = add_task(
                graph,
                created_tasks,
                &CollectorConfig::FileList,
                current_commit_hash,
                previous_commit_hash,
                previous_commit_distance,
            )?;

            graph.add_edge(
                dependency_node_idx,
                node_idx,
                CollectionGraphEdge { distance: 0 },
            );
        }
        CollectorConfig::ChangedFilesLoc => {
            let dependency_node_idx = add_task(
                graph,
                created_tasks,
                &CollectorConfig::ChangedFiles,
                current_commit_hash,
                previous_commit_hash,
                previous_commit_distance,
            )?;

            graph.add_edge(
                dependency_node_idx,
                node_idx,
                CollectionGraphEdge { distance: 0 },
            );
        }
        _ => {}
    }

    if let Some(previous_commit_hash) = previous_commit_hash {
        if let Some(last_commit_task_idx) =
            created_tasks.get(&(collector_config.clone(), previous_commit_hash.clone()))
        {
            graph.add_edge(
                *last_commit_task_idx,
                node_idx,
                CollectionGraphEdge {
                    distance: previous_commit_distance,
                },
            );
        }
    }

    Ok(node_idx)
}

pub fn build_collection_execution_graph(
    metrics: &HashMap<String, MetricConfig>,
    commits: &[CommitInfo],
    // Create a task for every metric for the latest commit,
    // regardless of the frequency specified in the metric config
    force_latest_commit: bool,
) -> Result<CollectionExecutionGraph> {
    let mut graph: Graph<CollectionTask, CollectionGraphEdge> = Graph::new();

    let mut sorted_commits = commits.to_vec();
    sorted_commits.sort_by(|a, b| {
        a.time
            .partial_cmp(&b.time)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut created_tasks: HashMap<(CollectorConfig, CommitHash), NodeIndex> = HashMap::new();
    for metric_config in metrics.values() {
        let mut distance = 0_usize;
        let mut previous_commit: Option<&CommitInfo> = None;

        for (index, current_commit) in sorted_commits.iter().enumerate() {
            let current_commit_hash = &current_commit.id;
            let is_latest_commit = index == sorted_commits.len() - 1;

            let skipped = if force_latest_commit && is_latest_commit {
                false
            } else if let Some(previous_commit) = previous_commit {
                let is_same_year = previous_commit.time.date_naive().year_ce()
                    == current_commit.time.date_naive().year_ce();

                let is_same_month = is_same_year
                    && previous_commit.time.date_naive().month0()
                        == current_commit.time.date_naive().month0();

                let is_same_week = is_same_month
                    && previous_commit.time.date_naive().iso_week()
                        == current_commit.time.date_naive().iso_week();

                let is_same_day = is_same_week
                    && previous_commit.time.date_naive().day0()
                        == current_commit.time.date_naive().day0();

                let is_same_hour =
                    is_same_day && previous_commit.time.hour() == current_commit.time.hour();

                match metric_config.frequency {
                    Frequency::PerCommit => false,
                    Frequency::Yearly => is_same_year,
                    Frequency::Monthly => is_same_month,
                    Frequency::Weekly => is_same_week,
                    Frequency::Daily => is_same_day,
                    Frequency::Hourly => is_same_hour,
                }
            } else {
                false
            };

            if skipped {
                distance += 1;
                continue;
            }

            add_task(
                &mut graph,
                &mut created_tasks,
                &metric_config.collector,
                current_commit_hash,
                previous_commit.map(|c| &c.id),
                distance,
            )?;

            previous_commit = Some(current_commit);
        }
    }

    Ok(CollectionExecutionGraph { graph })
}

#[cfg(test)]
mod test {
    use chrono::{DateTime, Utc};

    use crate::git::Author;

    use super::*;

    fn create_dummy_commit(hash: &str, time: &str) -> CommitInfo {
        let dummy_author: Author = Author {
            name: Some("Dummy".to_string()),
            email: Some("dummy@test.com".to_string()),
        };

        CommitInfo {
            id: CommitHash(hash.to_string()),
            author: dummy_author.clone(),
            committer: dummy_author.clone(),
            message: None,
            time: time.parse::<DateTime<Utc>>().unwrap(),
        }
    }

    #[test]
    fn test_build_collection_execution_graph_per_commit_without_force_latest() {
        let mut metrics = HashMap::new();

        metrics.insert(
            "test_metric".to_string(),
            MetricConfig {
                frequency: crate::Frequency::PerCommit,
                collector: CollectorConfig::Loc,
            },
        );

        let commits = vec![
            create_dummy_commit("1", "2012-12-12T00:00:00Z"),
            create_dummy_commit("2", "2012-12-13T00:00:00Z"),
            create_dummy_commit("3", "2012-12-14T00:00:00Z"),
            create_dummy_commit("4", "2012-12-15T00:00:00Z"),
            create_dummy_commit("5", "2012-12-16T00:00:00Z"),
        ];

        let result = build_collection_execution_graph(&metrics, &commits, false).unwrap();

        assert_eq!(result.graph.node_count(), commits.len());
        for c in commits {
            let node = result
                .graph
                .raw_nodes()
                .iter()
                .find(|n| n.weight.commit_hash == c.id);

            assert!(node.is_some())
        }
    }

    #[test]
    fn test_build_collection_execution_graph_daily_without_force_latest() {
        let mut metrics = HashMap::new();

        metrics.insert(
            "test_metric".to_string(),
            MetricConfig {
                frequency: crate::Frequency::Daily,
                collector: CollectorConfig::Loc,
            },
        );

        let commits = vec![
            create_dummy_commit("1.0", "2012-12-12T00:00:00Z"),
            create_dummy_commit("1.1", "2012-12-12T01:00:00Z"),
            create_dummy_commit("1.2", "2012-12-12T02:00:00Z"),
            create_dummy_commit("1.3", "2012-12-12T03:00:00Z"),
            create_dummy_commit("2", "2012-12-13T00:00:00Z"),
            create_dummy_commit("3.0", "2012-12-14T00:00:00Z"),
            create_dummy_commit("3.1", "2012-12-14T01:00:00Z"),
            create_dummy_commit("3.2", "2012-12-14T18:00:00Z"),
            create_dummy_commit("4", "2012-12-15T00:00:00Z"),
            create_dummy_commit("5.0", "2012-12-16T00:00:00Z"),
            create_dummy_commit("5.1", "2012-12-16T01:00:00Z"),
        ];

        let result = build_collection_execution_graph(&metrics, &commits, false).unwrap();

        assert_eq!(result.graph.node_count(), 5);
        for c in ["1.0", "2", "3.0", "4", "5.0"] {
            let node = result
                .graph
                .raw_nodes()
                .iter()
                .find(|n| n.weight.commit_hash == CommitHash(String::from(c)));

            assert!(node.is_some())
        }
    }

    #[test]
    fn test_build_collection_execution_graph_weekly_without_force_latest() {
        let mut metrics = HashMap::new();

        metrics.insert(
            "test_metric".to_string(),
            MetricConfig {
                frequency: crate::Frequency::Weekly,
                collector: CollectorConfig::Loc,
            },
        );

        let commits = vec![
            create_dummy_commit("1.0", "2024-07-02T00:00:00Z"),
            create_dummy_commit("1.1", "2024-07-02T12:00:00Z"),
            create_dummy_commit("1.2", "2024-07-05T00:00:00Z"),
            create_dummy_commit("2.0", "2024-07-08T00:00:00Z"),
            create_dummy_commit("3.0", "2024-07-15T00:00:00Z"),
            create_dummy_commit("4.0", "2024-07-24T00:00:00Z"),
        ];

        let result = build_collection_execution_graph(&metrics, &commits, false).unwrap();

        assert_eq!(result.graph.node_count(), 4);
        for c in ["1.0", "2.0", "3.0", "4.0"] {
            let node = result
                .graph
                .raw_nodes()
                .iter()
                .find(|n| n.weight.commit_hash == CommitHash(String::from(c)));

            assert!(node.is_some())
        }
    }

    #[test]
    fn test_build_collection_execution_graph_monthly_without_force_latest() {
        let mut metrics = HashMap::new();

        metrics.insert(
            "test_metric".to_string(),
            MetricConfig {
                frequency: crate::Frequency::Monthly,
                collector: CollectorConfig::Loc,
            },
        );

        let commits = vec![
            create_dummy_commit("1.0", "2012-12-12T00:00:00Z"),
            create_dummy_commit("1.1", "2012-12-13T01:00:00Z"),
            create_dummy_commit("1.2", "2012-12-13T12:10:00Z"),
            create_dummy_commit("2.0", "2013-01-18T12:10:00Z"),
            create_dummy_commit("3.0", "2013-02-18T12:10:00Z"),
            create_dummy_commit("4.0", "2013-05-18T12:10:00Z"),
            create_dummy_commit("4.1", "2013-05-19T10:00:00Z"),
        ];

        let result = build_collection_execution_graph(&metrics, &commits, false).unwrap();

        assert_eq!(result.graph.node_count(), 4);
        for c in ["1.0", "2.0", "3.0", "4.0"] {
            let node = result
                .graph
                .raw_nodes()
                .iter()
                .find(|n| n.weight.commit_hash == CommitHash(String::from(c)));

            assert!(node.is_some())
        }
    }

    #[test]
    fn test_build_collection_execution_graph_yearly_without_force_latest() {
        let mut metrics = HashMap::new();

        metrics.insert(
            "test_metric".to_string(),
            MetricConfig {
                frequency: crate::Frequency::Yearly,
                collector: CollectorConfig::Loc,
            },
        );

        let commits = vec![
            create_dummy_commit("2012#1", "2012-12-12T00:00:00Z"),
            create_dummy_commit("2012#2", "2012-12-12T01:00:00Z"),
            create_dummy_commit("2012#3", "2012-12-13T00:00:00Z"),
            create_dummy_commit("2013#1", "2013-02-06T00:00:00Z"),
            create_dummy_commit("2014#1", "2014-02-07T00:00:00Z"),
            create_dummy_commit("2014#2", "2014-03-01T14:00:00Z"),
            create_dummy_commit("2014#3", "2014-03-01T14:00:00Z"),
        ];

        let result = build_collection_execution_graph(&metrics, &commits, false).unwrap();

        assert_eq!(result.graph.node_count(), 3);
        for c in ["2012#1", "2013#1", "2014#1"] {
            let node = result
                .graph
                .raw_nodes()
                .iter()
                .find(|n| n.weight.commit_hash == CommitHash(String::from(c)));

            assert!(node.is_some())
        }
    }

    #[test]
    fn test_build_collection_execution_graph_per_commit_with_force_latest() {
        let mut metrics = HashMap::new();

        metrics.insert(
            "test_metric".to_string(),
            MetricConfig {
                frequency: crate::Frequency::PerCommit,
                collector: CollectorConfig::Loc,
            },
        );

        let commits = vec![
            create_dummy_commit("1", "2012-12-12T00:00:00Z"),
            create_dummy_commit("2", "2012-12-13T00:00:00Z"),
            create_dummy_commit("3", "2012-12-14T00:00:00Z"),
            create_dummy_commit("4", "2012-12-15T00:00:00Z"),
            create_dummy_commit("5", "2012-12-16T00:00:00Z"),
        ];

        let result = build_collection_execution_graph(&metrics, &commits, true).unwrap();

        assert_eq!(result.graph.node_count(), commits.len());
        for c in commits {
            let node = result
                .graph
                .raw_nodes()
                .iter()
                .find(|n| n.weight.commit_hash == c.id);

            assert!(node.is_some())
        }
    }

    #[test]
    fn test_build_collection_execution_graph_daily_with_force_latest() {
        let mut metrics = HashMap::new();

        metrics.insert(
            "test_metric".to_string(),
            MetricConfig {
                frequency: crate::Frequency::Daily,
                collector: CollectorConfig::Loc,
            },
        );

        let commits = vec![
            create_dummy_commit("1.0", "2012-12-12T00:00:00Z"),
            create_dummy_commit("1.1", "2012-12-12T01:00:00Z"),
            create_dummy_commit("1.2", "2012-12-12T02:00:00Z"),
            create_dummy_commit("1.3", "2012-12-12T03:00:00Z"),
            create_dummy_commit("2", "2012-12-13T00:00:00Z"),
            create_dummy_commit("3.0", "2012-12-14T00:00:00Z"),
            create_dummy_commit("3.1", "2012-12-14T01:00:00Z"),
            create_dummy_commit("3.2", "2012-12-14T18:00:00Z"),
            create_dummy_commit("4", "2012-12-15T00:00:00Z"),
            create_dummy_commit("5.0", "2012-12-16T00:00:00Z"),
            create_dummy_commit("5.1", "2012-12-16T01:00:00Z"),
        ];

        let result = build_collection_execution_graph(&metrics, &commits, true).unwrap();

        assert_eq!(result.graph.node_count(), 6);
        for c in ["1.0", "2", "3.0", "4", "5.0", "5.1"] {
            let node = result
                .graph
                .raw_nodes()
                .iter()
                .find(|n| n.weight.commit_hash == CommitHash(String::from(c)));

            assert!(node.is_some())
        }
    }

    #[test]
    fn test_build_collection_execution_graph_weekly_with_force_latest() {
        let mut metrics = HashMap::new();

        metrics.insert(
            "test_metric".to_string(),
            MetricConfig {
                frequency: crate::Frequency::Weekly,
                collector: CollectorConfig::Loc,
            },
        );

        let commits = vec![
            create_dummy_commit("1.0", "2024-07-02T00:00:00Z"),
            create_dummy_commit("1.1", "2024-07-02T12:00:00Z"),
            create_dummy_commit("1.2", "2024-07-05T00:00:00Z"),
            create_dummy_commit("2.0", "2024-07-08T00:00:00Z"),
            create_dummy_commit("3.0", "2024-07-15T00:00:00Z"),
            create_dummy_commit("4.0", "2024-07-24T00:00:00Z"),
            create_dummy_commit("4.1", "2024-07-24T01:00:00Z"),
        ];

        let result = build_collection_execution_graph(&metrics, &commits, true).unwrap();

        assert_eq!(result.graph.node_count(), 5);
        for c in ["1.0", "2.0", "3.0", "4.0", "4.1"] {
            let node = result
                .graph
                .raw_nodes()
                .iter()
                .find(|n| n.weight.commit_hash == CommitHash(String::from(c)));

            assert!(node.is_some())
        }
    }

    #[test]
    fn test_build_collection_execution_graph_monthly_with_force_latest() {
        let mut metrics = HashMap::new();

        metrics.insert(
            "test_metric".to_string(),
            MetricConfig {
                frequency: crate::Frequency::Monthly,
                collector: CollectorConfig::Loc,
            },
        );

        let commits = vec![
            create_dummy_commit("1.0", "2012-12-12T00:00:00Z"),
            create_dummy_commit("1.1", "2012-12-13T01:00:00Z"),
            create_dummy_commit("1.2", "2012-12-13T12:10:00Z"),
            create_dummy_commit("2.0", "2013-01-18T12:10:00Z"),
            create_dummy_commit("3.0", "2013-02-18T12:10:00Z"),
            create_dummy_commit("4.0", "2013-05-18T12:10:00Z"),
            create_dummy_commit("4.1", "2013-05-19T10:00:00Z"),
        ];

        let result = build_collection_execution_graph(&metrics, &commits, true).unwrap();

        assert_eq!(result.graph.node_count(), 5);
        for c in ["1.0", "2.0", "3.0", "4.0", "4.1"] {
            let node = result
                .graph
                .raw_nodes()
                .iter()
                .find(|n| n.weight.commit_hash == CommitHash(String::from(c)));

            assert!(node.is_some())
        }
    }

    #[test]
    fn test_build_collection_execution_graph_yearly_with_force_latest() {
        let mut metrics = HashMap::new();

        metrics.insert(
            "test_metric".to_string(),
            MetricConfig {
                frequency: crate::Frequency::Yearly,
                collector: CollectorConfig::Loc,
            },
        );

        let commits = vec![
            create_dummy_commit("2012#1", "2012-12-12T00:00:00Z"),
            create_dummy_commit("2012#2", "2012-12-12T01:00:00Z"),
            create_dummy_commit("2012#3", "2012-12-13T00:00:00Z"),
            create_dummy_commit("2013#1", "2013-02-06T00:00:00Z"),
            create_dummy_commit("2014#1", "2014-02-07T00:00:00Z"),
            create_dummy_commit("2014#2", "2014-03-01T14:00:00Z"),
            create_dummy_commit("2014#3", "2014-03-01T14:00:00Z"),
        ];

        let result = build_collection_execution_graph(&metrics, &commits, true).unwrap();

        assert_eq!(result.graph.node_count(), 4);
        for c in ["2012#1", "2013#1", "2014#1", "2014#3"] {
            let node = result
                .graph
                .raw_nodes()
                .iter()
                .find(|n| n.weight.commit_hash == CommitHash(String::from(c)));

            assert!(node.is_some())
        }
    }
}
