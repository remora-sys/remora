use rustc_hash::FxHashMap;
use std::sync::Arc;
use sui_types::base_types::ObjectID;
use tokio::sync::Notify;

pub type TaskID = u64;
/// Notify is similar to a channel but without sending any data.
pub type TaskHandle = (TaskID, Arc<Notify>);
pub type TaskEntry = Option<TaskHandle>;
pub type ObjectTaskMap = FxHashMap<ObjectID, TaskEntry>;

/// The dependency controller is reponsible for dynamically maintaining
/// inter-task dependency graph due to overlapped resource accesses.
pub struct DependencyController {
    /// This map contains the tail task of all priors ones
    /// which access the given object.
    obj_task_map: ObjectTaskMap,
}

impl DependencyController {
    pub async fn new() -> Self {
        let obj_task_map: ObjectTaskMap = FxHashMap::default();

        Self { obj_task_map }
    }

    /// Checks if a given `ObjectID` has an associated task.
    pub fn has_task_for_object(&self, obj_id: &ObjectID) -> bool {
        self.obj_task_map.contains_key(obj_id)
    }

    /// Get prior dependencies, generate new handles, and update the map
    pub async fn get_dependencies(
        &mut self,
        task_id: u64,
        obj_ids: Vec<ObjectID>,
    ) -> (Vec<Arc<Notify>>, Vec<Arc<Notify>>) {
        let mut prior_tasks = Vec::new();

        let current_tasks: Vec<_> = (0..obj_ids.len())
            .map(|_| Arc::new(Notify::new()))
            .collect();

        for (&obj_id, notify) in obj_ids.iter().zip(current_tasks.iter()) {
            if let Some(entry) = self.obj_task_map.get_mut(&obj_id) {
                if let Some((_, existing_notify)) = entry.take() {
                    prior_tasks.push(existing_notify);
                }

                *entry = Some((task_id, notify.clone()));
            } else {
                self.obj_task_map
                    .insert(obj_id, Some((task_id, notify.clone())));
            }
        }

        (prior_tasks, current_tasks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_no_prior_dependencies() {
        let mut dependency_controller = DependencyController::new().await;
        let task_id = 1;
        let obj_ids = vec![ObjectID::random(), ObjectID::random()];

        let (prior_tasks, current_tasks) = dependency_controller
            .get_dependencies(task_id, obj_ids.clone())
            .await;

        assert!(
            prior_tasks.is_empty(),
            "There should be no prior dependencies."
        );
        assert_eq!(
            current_tasks.len(),
            obj_ids.len(),
            "Should create a new notify for each object ID."
        );

        // Ensure that each ObjectID now has a corresponding entry in the map
        for obj_id in obj_ids {
            assert!(dependency_controller.has_task_for_object(&obj_id));
        }
    }

    #[tokio::test]
    async fn test_with_prior_dependencies_same_object() {
        let mut dependency_controller = DependencyController::new().await;
        let task_id1 = 1;
        let task_id2 = 2;
        let obj_id = ObjectID::random();

        // First task accesses the object ID
        let (prior_tasks, current_tasks1) = dependency_controller
            .get_dependencies(task_id1, vec![obj_id])
            .await;
        assert!(
            prior_tasks.is_empty(),
            "There should be no prior dependencies for the first task."
        );
        assert_eq!(
            current_tasks1.len(),
            1,
            "Should create a new notify for the object ID."
        );

        // Second task accesses the same object ID
        let (prior_tasks, current_tasks2) = dependency_controller
            .get_dependencies(task_id2, vec![obj_id])
            .await;
        assert_eq!(
            prior_tasks.len(),
            1,
            "The second task should see one prior dependency."
        );
        assert_eq!(
            current_tasks2.len(),
            1,
            "Should create a new notify for the object ID."
        );

        // Check if the prior notify is the same as the one created for task 1
        assert!(
            Arc::ptr_eq(&prior_tasks[0], &current_tasks1[0]),
            "The prior task's notify should match the first task's notify."
        );

        // Ensure the map has been updated to reflect task_id2 as the latest task
        if let Some(Some((id, _))) = dependency_controller.obj_task_map.get(&obj_id) {
            assert_eq!(*id, task_id2);
        } else {
            panic!("Entry for obj_id not found in obj_task_map");
        }
    }

    #[tokio::test]
    async fn test_multiple_objects_no_overlap() {
        let mut dependency_controller = DependencyController::new().await;
        let task_id1 = 1;
        let task_id2 = 2;
        let obj_ids1 = vec![ObjectID::random(), ObjectID::random()];
        let obj_ids2 = vec![ObjectID::random(), ObjectID::random()]; // Different object IDs

        // First task accesses a set of object IDs
        let (prior_tasks1, current_tasks1) = dependency_controller
            .get_dependencies(task_id1, obj_ids1.clone())
            .await;
        assert!(
            prior_tasks1.is_empty(),
            "There should be no prior dependencies for the first task."
        );
        assert_eq!(
            current_tasks1.len(),
            obj_ids1.len(),
            "Should create new notifies for each object ID."
        );

        // Second task accesses a different set of object IDs
        let (prior_tasks2, current_tasks2) = dependency_controller
            .get_dependencies(task_id2, obj_ids2.clone())
            .await;
        assert!(
            prior_tasks2.is_empty(),
            "There should be no prior dependencies for the second task."
        );
        assert_eq!(
            current_tasks2.len(),
            obj_ids2.len(),
            "Should create new notifies for each object ID."
        );

        // Ensure the object map has entries for all object IDs
        for obj_id in obj_ids1.iter().chain(obj_ids2.iter()) {
            assert!(
                dependency_controller.obj_task_map.contains_key(obj_id),
                "Object ID should have an entry in the task map."
            );
        }
    }

    #[tokio::test]
    async fn test_partial_prior_dependencies() {
        let mut dependency_controller = DependencyController::new().await;
        let task_id1 = 1;
        let task_id2 = 2;
        let obj_ids1 = vec![ObjectID::random(), ObjectID::random()];
        let obj_ids2 = vec![obj_ids1[1], ObjectID::random()]; // One overlapping ObjectID

        // First task accesses the first set of object IDs
        let (prior_tasks1, current_tasks1) = dependency_controller
            .get_dependencies(task_id1, obj_ids1.clone())
            .await;
        assert!(
            prior_tasks1.is_empty(),
            "There should be no prior dependencies for the first task."
        );
        assert_eq!(
            current_tasks1.len(),
            obj_ids1.len(),
            "Should create new notifies for each object ID."
        );

        // Second task accesses a set with one overlapping ObjectID
        let (prior_tasks2, current_tasks2) = dependency_controller
            .get_dependencies(task_id2, obj_ids2.clone())
            .await;
        assert_eq!(
            prior_tasks2.len(),
            1,
            "There should be one prior dependency for the overlapping ObjectID."
        );
        assert_eq!(
            current_tasks2.len(),
            obj_ids2.len(),
            "Should create new notifies for each object ID."
        );

        // Ensure that the prior notify corresponds to the overlapping ObjectID
        assert!(
            Arc::ptr_eq(&prior_tasks2[0], &current_tasks1[1]),
            "The prior notify should match the one for the overlapping ObjectID."
        );

        // Ensure the map has been updated correctly
        for (obj_id, notify) in obj_ids2.iter().zip(current_tasks2.iter()) {
            if let Some(Some((id, n))) = dependency_controller.obj_task_map.get(obj_id) {
                assert_eq!(
                    *id, task_id2,
                    "Task ID in the map should be updated to task_id2."
                );
                assert!(
                    Arc::ptr_eq(n, notify),
                    "The notify in the map should match the current notify."
                );
            } else {
                panic!("Entry for obj_id not found in obj_task_map");
            }
        }
    }
}
