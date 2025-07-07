package repositories

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"log"
	"os"
	"task--service/model"
	"time"
)

type TaskRepository struct {
	cli    *mongo.Client
	logger *log.Logger
	tracer trace.Tracer
}

const (
	taskSuccess = "Successfully got all tasks"
	taskUpdate  = "Successfully updated the task"
)

func New(ctx context.Context, logger *log.Logger, trace trace.Tracer) (*TaskRepository, error) {
	dburi := os.Getenv("MONGO_DB_URI")
	if dburi == "" {
		return nil, fmt.Errorf("MONGO_DB_URI environment variable is not set")
	}

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(dburi))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	if err = client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	logger.Println("Successfully connected to MongoDB")

	return &TaskRepository{
		cli:    client,
		logger: logger,
		tracer: trace,
	}, nil
}

func (tr *TaskRepository) Disconnect(ctx context.Context) error {
	ctx, span := tr.tracer.Start(ctx, "TaskRepository.Disconnect")
	defer span.End()
	err := tr.cli.Disconnect(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetStatus(codes.Ok, "Successfully disconnected")
	return nil
}

func (tr *TaskRepository) getCollection() *mongo.Collection {
	projectDatabase := tr.cli.Database("mongoTask")
	projectCollection := projectDatabase.Collection("tasks")
	return projectCollection
}

func (tr *TaskRepository) Ping() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := tr.cli.Ping(ctx, readpref.Primary())
	if err != nil {
		tr.logger.Println(err)
	}

	databases, err := tr.cli.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		tr.logger.Println(err)
	}
	fmt.Println(databases)
}

func (tr *TaskRepository) Insert(ctx context.Context, task *model.Task) error {
	ctx, span := tr.tracer.Start(ctx, "TaskRepository.Insert")
	defer span.End()
	tasksCollection := tr.getCollection()

	if task.UserIDs == nil {
		task.UserIDs = []string{}
	}

	if task.Status == "" {
		task.Status = model.Pending
	}

	task.CreatedAt = time.Now()
	task.UpdatedAt = time.Now()

	insertOneCtx, insertOneSpan := tr.tracer.Start(ctx, "TaskRepository.Insert.InsertOne")
	result, err := tasksCollection.InsertOne(insertOneCtx, task)
	if oid, ok := result.InsertedID.(primitive.ObjectID); ok {
		task.ID = oid
	} else {
		tr.logger.Println("Failed to convert InsertedID to ObjectID")
	}
	insertOneSpan.End()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		tr.logger.Println(err)
		return err
	}
	tr.logger.Printf("Task created with ID: %v\n", result.InsertedID)
	span.SetStatus(codes.Ok, "Successfully inserted a task")
	return nil
}

func (tr *TaskRepository) GetAllTask(ctx context.Context) (model.Tasks, error) {
	ctx, span := tr.tracer.Start(ctx, "TaskRepository.GetAllTask")
	defer span.End()

	tasksCollection := tr.getCollection()

	var tasks model.Tasks
	tasksCursor, err := tasksCollection.Find(ctx, bson.M{})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		tr.logger.Println(err)
		return nil, err
	}
	if err = tasksCursor.All(ctx, &tasks); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		tr.logger.Println(err)
		return nil, err
	}
	span.SetStatus(codes.Ok, taskSuccess)
	return tasks, nil
}

func (tr *TaskRepository) GetDependenciesByTaskId(ctx context.Context, taskID string) ([]model.Task, error) {
	ctx, span := tr.tracer.Start(ctx, "TaskRepository.GetDependenciesByTaskId")
	defer span.End()
	var tasks []model.Task
	filter := bson.M{"_id": taskID}

	tasksCollection := tr.getCollection()
	tasksCursor, err := tasksCollection.Find(ctx, filter)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	defer tasksCursor.Close(ctx)

	for tasksCursor.Next(ctx) {
		var task model.Task
		if err := tasksCursor.Decode(&task); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		tasks = append(tasks, task)
	}
	span.SetStatus(codes.Ok, taskSuccess)
	return tasks, tasksCursor.Err()
}

func (tr *TaskRepository) GetAllByProjectId(ctx context.Context, projectID string) ([]model.Task, error) {

	var tasks []model.Task

	filter := bson.M{"project_id": projectID}

	ctx, span := tr.tracer.Start(ctx, "TaskRepository.GetAllByProjectIdProject")
	defer span.End()

	tasksCollection := tr.getCollection()
	tasksCursor, err := tasksCollection.Find(ctx, filter)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return tasks, err
	}
	defer tasksCursor.Close(ctx)

	for tasksCursor.Next(ctx) {
		var task model.Task
		if err := tasksCursor.Decode(&task); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return tasks, err
		}
		tasks = append(tasks, task)
	}

	if err := tasksCursor.Err(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return tasks, err
	}
	span.SetStatus(codes.Ok, taskSuccess)
	return tasks, nil

}

func (tr *TaskRepository) DeleteTask(ctx context.Context, taskId primitive.ObjectID) error {
	ctx, span := tr.tracer.Start(ctx, "TaskRepository.DeleteTask")
	defer span.End()
	collection := tr.getCollection()
	_, err := collection.DeleteOne(ctx, bson.M{"_id": taskId})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("Failed to delete task with id %s: %v", taskId.Hex(), err)
	}
	span.SetStatus(codes.Ok, "Successfully deleted the task")
	return nil
}

func (t *TaskRepository) DeleteAllTasksByProjectId(ctx context.Context, projectID string) error {

	ctx, span := t.tracer.Start(ctx, "TaskRepository.DeleteAllTasksByProjectId")
	defer span.End()
	tasks, err := t.GetAllByProjectId(ctx, projectID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		t.logger.Printf("Failed to fetch tasks for project %s: %v", projectID, err)
		return fmt.Errorf("failed to fetch tasks: %w", err)
	}

	for _, task := range tasks {
		err := t.DeleteTask(ctx, task.ID)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			t.logger.Printf("Failed to delete task with ID %s: %v", task.ID.Hex(), err)
			return fmt.Errorf("failed to delete task with ID %s: %w", task.ID.Hex(), err)
		}
	}

	t.logger.Printf("Successfully deleted all tasks for project %s", projectID)
	span.SetStatus(codes.Ok, "Successfully deleted all tasks")
	return nil
}

func (t *TaskRepository) UpdateAllTasksByProjectId(ctx context.Context, projectID string, toDelete bool) error {

	_, span := t.tracer.Start(context.Background(), "TaskRepository.DeleteAllTasksByProjectId")
	defer span.End()
	tasks, err := t.GetAllByProjectId(ctx, projectID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		t.logger.Printf("Failed to fetch tasks for project %s: %v", projectID, err)
		return fmt.Errorf("failed to fetch tasks: %w", err)
	}

	for _, task := range tasks {
		task.PendingDeletion = toDelete
		err := t.UpdatePendingDeletion(&task)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			t.logger.Printf("Failed to delete task with ID %s: %v", task.ID.Hex(), err)
			return fmt.Errorf("failed to delete task with ID %s: %w", task.ID.Hex(), err)
		}
	}

	t.logger.Printf("Successfully updated all tasks for project %s", projectID)
	span.SetStatus(codes.Ok, "Successfully updateds all tasks")
	return nil
}

func (tr *TaskRepository) GetByID(ctx context.Context, taskID string) (*model.Task, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, span := tr.tracer.Start(ctx, "TaskRepository.GetByID")

	defer span.End()
	tasksCollection := tr.getCollection()

	taskObjID, err := primitive.ObjectIDFromHex(taskID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("invalid taskID format: %v", err)
	}

	var task model.Task
	err = tasksCollection.FindOne(ctx, bson.M{"_id": taskObjID}).Decode(&task)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetStatus(codes.Ok, "Successfully got task")
	return &task, nil
}

func (tr *TaskRepository) Update(ctx context.Context, task *model.Task) error {
	ctx, span := tr.tracer.Start(ctx, "TaskRepository.Update")
	defer span.End()

	tasksCollection := tr.getCollection()
	filter := bson.M{"_id": task.ID}
	update := bson.M{
		"$set": bson.M{
			"user_ids":   task.UserIDs,
			"updated_at": time.Now(),
		},
	}

	_, err := tasksCollection.UpdateOne(ctx, filter, update)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetStatus(codes.Ok, taskUpdate)
	return nil
}

func (tr *TaskRepository) UpdatePendingDeletion(task *model.Task) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, span := tr.tracer.Start(ctx, "TaskRepository.Update")
	defer span.End()

	tasksCollection := tr.getCollection()
	filter := bson.M{"_id": task.ID}
	update := bson.M{
		"$set": bson.M{
			"pending_deletion": task.PendingDeletion,
			"updated_at":       time.Now(),
		},
	}

	_, err := tasksCollection.UpdateOne(ctx, filter, update)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetStatus(codes.Ok, taskUpdate)
	return nil
}

func (tr *TaskRepository) UpdateFlag(ctx context.Context, task *model.Task, blocked bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, span := tr.tracer.Start(ctx, "TaskRepository.UpdateFlag")

	defer span.End()

	tasksCollection := tr.getCollection()
	filter := bson.M{"_id": task.ID}
	update := bson.M{
		"$set": bson.M{
			"blocked":    blocked,
			"updated_at": time.Now(),
		},
	}

	_, err := tasksCollection.UpdateOne(ctx, filter, update)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetStatus(codes.Ok, "Successfully updated the task flag")
	return nil
}

func (tr *TaskRepository) AddDependency(ctx context.Context, task *model.Task, dependencyID string) error {
	ctx, span := tr.tracer.Start(ctx, "TaskRepository.UpdateDependency")
	defer span.End()
	task.Dependencies = append(task.Dependencies, dependencyID)
	tr.logger.Println("new dependencies: ", task.Dependencies)
	tasksCollection := tr.getCollection()
	filter := bson.M{"_id": task.ID}
	update := bson.M{
		"$set": bson.M{
			"dependencies": task.Dependencies,
			"updated_at":   time.Now(),
		},
	}

	_, err := tasksCollection.UpdateOne(ctx, filter, update)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetStatus(codes.Ok, "Successfully updated the task flag")
	return nil
}

func (tr *TaskRepository) InsertTaskMemberActivity(ctx context.Context, change *model.TaskMemberActivity) error {
	ctx, span := tr.tracer.Start(ctx, "TaskRepository.InsertTaskMemberActivity")
	defer span.End()
	_, err := tr.getChangeCollection().InsertOne(ctx, change)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetStatus(codes.Ok, "Successfully inserted the task member activity")
	return nil
}

func (tr *TaskRepository) GetUnprocessedActivities(ctx context.Context) ([]model.TaskMemberActivity, error) {
	ctx, span := tr.tracer.Start(ctx, "TaskRepository.GetUnprocessedActivities")
	defer span.End()

	cursor, err := tr.getChangeCollection().Find(ctx, bson.M{"processed": false})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	var changes []model.TaskMemberActivity
	err = cursor.All(ctx, &changes)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetStatus(codes.Ok, "Successfully got all task member activity")
	return changes, nil
}

func (tr *TaskRepository) MarkChangeAsProcessed(ctx context.Context, changeID string) error {
	ctx, span := tr.tracer.Start(ctx, "TaskRepository.MarkChangeAsProcessed")
	defer span.End()

	_, err := tr.getChangeCollection().UpdateOne(ctx, bson.M{"_id": changeID}, bson.M{"$set": bson.M{"processed": true}})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetStatus(codes.Ok, "Successfully marked as processed")
	return nil
}

func (tr *TaskRepository) getChangeCollection() *mongo.Collection {
	projectDatabase := tr.cli.Database("mongoTask")
	changeCollection := projectDatabase.Collection("task_member_activity")
	return changeCollection
}

func (tr *TaskRepository) UpdateStatus(ctx context.Context, task *model.Task, id string) error {
	ctx, span := tr.tracer.Start(ctx, "TaskRepository.UpdateStatus")
	defer span.End()

	objID, err := primitive.ObjectIDFromHex(task.ID.Hex())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("invalid task ID: %v", err)
	}

	isAssigned := false
	for _, userId := range task.UserIDs {
		if userId == id {
			isAssigned = true
			break
		}
	}
	if !isAssigned {
		span.RecordError(errors.New("User is not assigned to the task"))
		span.SetStatus(codes.Error, "User is not assigned to the task")
		return fmt.Errorf("user is not assigned to the task")
	}

	tasksCollection := tr.getCollection()
	filter := bson.M{"_id": objID}
	update := bson.M{
		"$set": bson.M{
			"status":     task.Status,
			"updated_at": time.Now(),
		},
	}

	result, err := tasksCollection.UpdateOne(ctx, filter, update)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("failed to update task status: %v", err)
	}

	if result.MatchedCount == 0 {
		span.SetStatus(codes.Error, "no task found")
		return fmt.Errorf("no task found with the given ID")
	}
	span.SetStatus(codes.Ok, taskUpdate)

	return nil
}

func (tr *TaskRepository) UnblockDependencies(task *model.Task) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, span := tr.tracer.Start(ctx, "TaskRepository.UnblockDependencies")
	defer span.End()

	tasksCollection := tr.getCollection()

	if len(task.Dependencies) == 0 {
		span.SetStatus(codes.Ok, "No dependencies to update")
		return nil
	}

	var dependencyIDs []primitive.ObjectID
	for _, dep := range task.Dependencies {
		objectID, err := primitive.ObjectIDFromHex(dep)
		if err != nil {
			tr.logger.Printf("Invalid dependency ID: %s", dep)
			continue
		}
		dependencyIDs = append(dependencyIDs, objectID)
	}
	dependencyFilter := bson.M{"_id": bson.M{"$in": dependencyIDs}}
	dependencyUpdate := bson.M{
		"$set": bson.M{
			"blocked":    false,
			"updated_at": time.Now(),
		},
	}
	tr.logger.Println("Successfully updated dependencies")
	tr.logger.Printf("Filter: %v, Update: %v", dependencyFilter, dependencyUpdate)

	updateResult, err := tasksCollection.UpdateMany(ctx, dependencyFilter, dependencyUpdate)
	if err != nil {
		tr.logger.Println("failed to update dependencies: %v", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("failed to update dependencies: %v", err)
	}

	if updateResult.MatchedCount == 0 {
		span.SetStatus(codes.Error, "no dependencies found")
		tr.logger.Println("no dependencies found to update")

		return fmt.Errorf("no dependencies found to update")
	}
	tr.logger.Println("Successfully unblocked dependencies")

	span.SetStatus(codes.Ok, "Successfully unblocked dependencies")
	return nil

}
