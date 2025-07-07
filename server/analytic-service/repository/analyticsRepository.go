package repository

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"log"
	"os"
	"time"
)

type AnalyticsRepo struct {
	cli    *mongo.Client
	logger *log.Logger
	tracer trace.Tracer
}

const (
	usersOnProject           = "users_on_project.user_id"
	errFetchingProjectFormat = "Error fetching project: projectID=%s, error=%v"
	errGeneric               = "[ERROR] %s"
	taskSpent                = "tasks_time_spent.task_id"
)

func NewAnalyticsRepo(ctx context.Context, logger *log.Logger, tracer trace.Tracer) (*AnalyticsRepo, error) {
	dburi := os.Getenv("MONGO_DB_URI")
	if dburi == "" {
		return nil, fmt.Errorf("MONGO_DB_URI environment variable is not set")
	}

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(dburi))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	logger.Println("Successfully connected to MongoDB")
	return &AnalyticsRepo{
		cli:    client,
		logger: logger,
		tracer: tracer,
	}, nil
}

func (ar *AnalyticsRepo) Disconnect(ctx context.Context) error {
	_, span := ar.tracer.Start(ctx, "AnalyticsRepo.Disconnect")
	defer span.End()
	if err := ar.cli.Disconnect(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetStatus(codes.Ok, "Connection to MongoDB closed.")
	return nil
}

func (ar *AnalyticsRepo) CreateProject(ctx context.Context, projectId string, endDate string) error {
	_, span := ar.tracer.Start(ctx, "AnalyticsRepo.CreateProject")
	defer span.End()

	ar.logger.Printf("Creating project with ID: %s, End Date: %s", projectId, endDate)
	span.AddEvent(fmt.Sprintf("Creating project with ID: %s, End Date: %s", projectId, endDate))

	collection := ar.cli.Database("analytics_db").Collection("analytics")
	project := bson.M{
		"project_id":          projectId,
		"total_tasks":         0,
		"tasks_by_status":     bson.M{"Pending": 0, "In Progress": 0, "Completed": 0},
		"tasks_time_spent":    []bson.M{},
		"users_on_project":    []bson.M{},
		"project_on_schedule": nil,
		"completion_estimate": endDate,
		"last_updated":        time.Now(),
	}

	_, err := collection.InsertOne(ctx, project)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to create project")
		ar.logger.Printf("Error creating project: %v", err)
		return err
	}

	ar.logger.Println("Project created successfully")
	span.SetStatus(codes.Ok, "Project created successfully")
	return nil
}

func (ar *AnalyticsRepo) AddMemberToProject(ctx context.Context, projectID string, memberID string) error {
	_, span := ar.tracer.Start(ctx, "AnalyticsRepo.AddMemberToProject")
	defer span.End()

	ar.logger.Printf("Adding member with ID: %s to project with ID: %s", memberID, projectID)
	span.AddEvent(fmt.Sprintf("Adding member with ID: %s to project with ID: %s", memberID, projectID))

	collection := ar.cli.Database("analytics_db").Collection("analytics")

	filter := bson.M{"project_id": projectID, usersOnProject: bson.M{"$ne": memberID}}
	update := bson.M{
		"$push": bson.M{"users_on_project": bson.M{"user_id": memberID, "tasks_assigned": []interface{}{}}},
		"$set":  bson.M{"last_updated": time.Now()},
	}

	_, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to add member to project")
		ar.logger.Printf("Error adding member to project: %v", err)
		return err
	}

	ar.logger.Println("Member successfully added to project")
	span.SetStatus(codes.Ok, "Member added to project successfully")
	return nil
}

func (ar *AnalyticsRepo) AssignTaskToUser(ctx context.Context, projectID string, taskID string, memberID string) error {
	_, span := ar.tracer.Start(ctx, "AnalyticsRepo.AssignTaskToUser")
	defer span.End()

	ar.logger.Printf("Assigning task with ID: %s to member with ID: %s in project with ID: %s", taskID, memberID, projectID)
	span.AddEvent(fmt.Sprintf("Assigning task with ID: %s to member with ID: %s in project with ID: %s", taskID, memberID, projectID))

	collection := ar.cli.Database("analytics_db").Collection("analytics")

	filter := bson.M{"project_id": projectID, usersOnProject: memberID}
	update := bson.M{
		"$push": bson.M{"users_on_project.$.tasks_assigned": bson.M{"task_id": taskID}},
		"$set":  bson.M{"last_updated": time.Now()},
	}

	_, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to assign task to user")
		ar.logger.Printf("Error assigning task to user: %v", err)
		return err
	}

	ar.logger.Println("Task successfully assigned to user")
	span.SetStatus(codes.Ok, "Task assigned to user successfully")
	return nil
}

func (ar *AnalyticsRepo) RemoveProjectMember(ctx context.Context, projectID string, memberID string) error {
	_, span := ar.tracer.Start(ctx, "AnalyticsRepo.RemoveProjectMember")
	defer span.End()

	ar.logger.Printf("Removing member with ID: %s from project with ID: %s", memberID, projectID)
	span.AddEvent(fmt.Sprintf("Removing member with ID: %s from project with ID: %s", memberID, projectID))

	collection := ar.cli.Database("analytics_db").Collection("analytics")

	filter := bson.M{"project_id": projectID}
	update := bson.M{
		"$pull": bson.M{"users_on_project": bson.M{"user_id": memberID}},
		"$set":  bson.M{"last_updated": time.Now()},
	}

	_, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to remove project member")
		ar.logger.Printf("Error removing project member: %v", err)
		return err
	}

	ar.logger.Println("Member successfully removed from project")
	span.SetStatus(codes.Ok, "Member removed from project successfully")
	return nil
}

func (ar *AnalyticsRepo) RemoveTaskMember(ctx context.Context, projectID string, taskID string, memberID string) error {
	_, span := ar.tracer.Start(ctx, "AnalyticsRepo.RemoveTaskMember")
	defer span.End()

	ar.logger.Printf("Removing task with ID: %s from member with ID: %s in project with ID: %s", taskID, memberID, projectID)
	span.AddEvent(fmt.Sprintf("Removing task with ID: %s from member with ID: %s in project with ID: %s", taskID, memberID, projectID))

	collection := ar.cli.Database("analytics_db").Collection("analytics")

	filter := bson.M{"project_id": projectID, usersOnProject: memberID}
	update := bson.M{
		"$pull": bson.M{"users_on_project.$.tasks_assigned": bson.M{"task_id": taskID}},
		"$set":  bson.M{"last_updated": time.Now()},
	}

	_, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to remove task from member")
		ar.logger.Printf("Error removing task from member: %v", err)
		return err
	}

	ar.logger.Println("Task successfully removed from member")
	span.SetStatus(codes.Ok, "Task removed from member successfully")
	return nil
}

func (ar *AnalyticsRepo) CreateTask(ctx context.Context, projectID string, taskID string) error {
	_, span := ar.tracer.Start(ctx, "AnalyticsRepo.CreateTask")
	defer span.End()

	ar.logger.Printf("Creating task with ID: %s for project with ID: %s", taskID, projectID)
	span.AddEvent(fmt.Sprintf("Creating task with ID: %s for project with ID: %s", taskID, projectID))

	collection := ar.cli.Database("analytics_db").Collection("analytics")

	filter := bson.M{"project_id": projectID}
	update := bson.M{
		"$inc": bson.M{"total_tasks": 1, "tasks_by_status.Pending": 1},
		"$push": bson.M{"tasks_time_spent": bson.M{
			"task_id": taskID,
			"time_in_state": []bson.M{
				{
					"state": "Pending",
					"start": time.Now(),
				},
			},
		}},
		"$set": bson.M{"last_updated": time.Now()},
	}

	_, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to create task")
		ar.logger.Printf("Error creating task: %v", err)
		return err
	}

	ar.logger.Println("Task successfully created and added to project")
	span.SetStatus(codes.Ok, "Task created successfully")
	return nil
}

func (ar *AnalyticsRepo) UpdateTaskStatus(ctx context.Context, projectID string, taskID string, newStatus string) error {
	_, span := ar.tracer.Start(ctx, "AnalyticsRepo.UpdateTaskStatus")
	defer span.End()

	ar.logger.Printf("[INFO] Updating task status: taskID=%s, projectID=%s, newStatus=%s", taskID, projectID, newStatus)

	_, err := ar.fetchProject(ctx, projectID, span)
	if err != nil {
		return err
	}

	task, err := ar.fetchTask(ctx, projectID, taskID, span)
	if err != nil {
		return err
	}

	prevStatus, taskIndex, err := ar.getPreviousStatus(task, taskID)
	if err != nil {
		return err
	}

	if err := ar.updatePreviousStateEnd(ctx, projectID, taskID, taskIndex); err != nil {
		return err
	}

	if err := ar.pushNewState(ctx, projectID, taskID, newStatus, prevStatus, taskIndex); err != nil {
		return err
	}

	if err := ar.UpdateProjectOnSchedule(ctx, projectID); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ar.logger.Printf(errGeneric, err.Error())
		return err
	}

	span.SetStatus(codes.Ok, "Task status updated successfully")
	ar.logger.Printf("[INFO] Successfully updated task status: projectID=%s, taskID=%s, newStatus=%s", projectID, taskID, newStatus)
	return nil
}

func (ar *AnalyticsRepo) fetchProject(ctx context.Context, projectID string, span trace.Span) (bson.M, error) {
	span.AddEvent("Fetching project from DB", trace.WithAttributes(attribute.String("projectID", projectID)))
	collection := ar.cli.Database("analytics_db").Collection("analytics")

	var project bson.M
	err := collection.FindOne(ctx, bson.M{"project_id": projectID}).Decode(&project)
	if err != nil {
		errMsg := fmt.Sprintf(errFetchingProjectFormat, projectID, err)
		ar.logger.Printf(errGeneric, errMsg)
		span.RecordError(err)
		span.SetStatus(codes.Error, errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	return project, nil
}

func (ar *AnalyticsRepo) fetchTask(ctx context.Context, projectID string, taskID string, span trace.Span) (bson.M, error) {
	span.AddEvent("Fetching task details", trace.WithAttributes(attribute.String("taskID", taskID)))
	collection := ar.cli.Database("analytics_db").Collection("analytics")

	var task bson.M
	err := collection.FindOne(ctx, bson.M{
		"project_id": projectID,
		taskSpent:    taskID,
	}).Decode(&task)
	if err != nil {
		errMsg := fmt.Sprintf("Error fetching task: projectID=%s, taskID=%s, error=%v", projectID, taskID, err)
		ar.logger.Printf(errGeneric, errMsg)
		span.RecordError(err)
		span.SetStatus(codes.Error, errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	return task, nil
}

func (ar *AnalyticsRepo) getPreviousStatus(task bson.M, taskID string) (string, int, error) {
	tasksTimeSpent, ok := task["tasks_time_spent"].(primitive.A)
	if !ok {
		errMsg := fmt.Sprintf("Invalid tasks_time_spent structure: taskID=%s", taskID)
		ar.logger.Printf(errGeneric, errMsg)
		return "", -1, fmt.Errorf(errMsg)
	}

	var prevStatus string
	var taskIndex int
	for i, taskEntry := range tasksTimeSpent {
		taskMap, ok := taskEntry.(primitive.M)
		if !ok {
			continue
		}
		if taskMap["task_id"] == taskID {
			taskIndex = i
			timeInState, ok := taskMap["time_in_state"].(primitive.A)
			if ok && len(timeInState) > 0 {
				lastState, _ := timeInState[len(timeInState)-1].(primitive.M)
				if lastState != nil {
					prevStatus, _ = lastState["state"].(string)
				}
			}
			break
		}
	}

	if prevStatus == "" {
		errMsg := fmt.Sprintf("Previous task status missing: taskID=%s", taskID)
		ar.logger.Printf(errGeneric, errMsg)
		return "", -1, fmt.Errorf(errMsg)
	}

	return prevStatus, taskIndex, nil
}

func (ar *AnalyticsRepo) updatePreviousStateEnd(ctx context.Context, projectID string, taskID string, taskIndex int) error {
	collection := ar.cli.Database("analytics_db").Collection("analytics")
	filter := bson.M{"project_id": projectID, taskSpent: taskID}

	updateEnd := bson.M{
		"$set": bson.M{
			fmt.Sprintf("tasks_time_spent.%d.time_in_state.$[lastState].end", taskIndex): time.Now(),
		},
	}

	arrayFilters := options.ArrayFilters{
		Filters: []interface{}{
			bson.M{"lastState.end": bson.M{"$exists": false}},
		},
	}

	opts := options.Update().SetArrayFilters(arrayFilters)
	_, err := collection.UpdateOne(ctx, filter, updateEnd, opts)
	if err != nil {
		errMsg := fmt.Sprintf("Error updating previous state end time: projectID=%s, taskID=%s, error=%v", projectID, taskID, err)
		ar.logger.Printf(errGeneric, errMsg)
		return fmt.Errorf(errMsg)
	}
	return nil
}

func (ar *AnalyticsRepo) pushNewState(ctx context.Context, projectID string, taskID string, newStatus string, prevStatus string, taskIndex int) error {
	collection := ar.cli.Database("analytics_db").Collection("analytics")
	filter := bson.M{"project_id": projectID, taskSpent: taskID}

	updatePush := bson.M{
		"$push": bson.M{
			fmt.Sprintf("tasks_time_spent.%d.time_in_state", taskIndex): bson.M{
				"state": newStatus,
				"start": time.Now(),
			},
		},
		"$inc": bson.M{
			fmt.Sprintf("tasks_by_status.%s", prevStatus): -1,
			fmt.Sprintf("tasks_by_status.%s", newStatus):  1,
		},
		"$set": bson.M{
			"last_updated": time.Now(),
		},
	}

	_, err := collection.UpdateOne(ctx, filter, updatePush)
	if err != nil {
		errMsg := fmt.Sprintf("Error pushing new state: projectID=%s, taskID=%s, error=%v", projectID, taskID, err)
		ar.logger.Printf(errGeneric, errMsg)
		return fmt.Errorf(errMsg)
	}
	return nil
}

func (ar *AnalyticsRepo) UpdateProjectOnSchedule(ctx context.Context, projectID string) error {
	_, span := ar.tracer.Start(ctx, "AnalyticsRepo.UpdateProjectOnSchedule")
	defer span.End()

	ar.logger.Printf("[INFO] Updating project schedule status for projectID=%s", projectID)

	collection := ar.cli.Database("analytics_db").Collection("analytics")

	var project bson.M
	err := collection.FindOne(ctx, bson.M{"project_id": projectID}).Decode(&project)
	if err != nil {
		errMsg := fmt.Sprintf(errFetchingProjectFormat, projectID, err)
		span.RecordError(err)
		ar.logger.Printf(errGeneric, errMsg)
		return fmt.Errorf(errMsg)
	}

	estimatedEndDateStr, ok := project["completion_estimate"].(string)
	if !ok {
		errMsg := "Invalid completion_estimate format"
		span.RecordError(fmt.Errorf(errMsg))
		ar.logger.Printf(errGeneric, errMsg)
		return fmt.Errorf(errMsg)
	}

	estimatedEndDate, err := time.Parse(time.RFC3339, estimatedEndDateStr)
	if err != nil {
		errMsg := fmt.Sprintf("Error parsing estimated_end_date: %s", estimatedEndDateStr)
		span.RecordError(err)
		ar.logger.Printf(errGeneric, errMsg)
		return fmt.Errorf(errMsg)
	}

	if time.Now().After(estimatedEndDate) {
		ar.logger.Printf("[INFO] Setting project_on_schedule to false for projectID=%s: Estimated end date passed", projectID)

		update := bson.M{
			"$set": bson.M{
				"project_on_schedule": false,
				"last_updated":        time.Now(),
			},
		}

		_, err := collection.UpdateOne(ctx, bson.M{"project_id": projectID}, update)
		if err != nil {
			ar.logger.Printf("[ERROR] Error updating project on schedule status: %v", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, "Error updating project on schedule status")
			return err
		}

		ar.logger.Printf("[INFO] Successfully updated project_on_schedule to false for projectID=%s", projectID)
		return nil
	}

	tasksByStatus, ok := project["tasks_by_status"].(primitive.M)
	if !ok {
		errMsg := "Invalid tasks_by_status format"
		span.RecordError(fmt.Errorf(errMsg))
		ar.logger.Printf(errGeneric, errMsg)
		return fmt.Errorf(errMsg)
	}

	pending, _ := tasksByStatus["Pending"].(int32)
	inProgress, _ := tasksByStatus["In Progress"].(int32)
	completed, _ := tasksByStatus["Completed"].(int32)

	var projectOnSchedule interface{}
	if pending == 0 && inProgress == 0 && completed > 0 {
		projectOnSchedule = true
	} else {
		projectOnSchedule = nil
	}

	_, err = collection.UpdateOne(ctx, bson.M{"project_id": projectID}, bson.M{
		"$set": bson.M{
			"project_on_schedule": projectOnSchedule,
			"last_updated":        time.Now(),
		},
	})

	if err != nil {
		errMsg := fmt.Sprintf("Error updating project schedule status: projectID=%s, error=%v", projectID, err)
		span.RecordError(err)
		ar.logger.Printf(errGeneric, errMsg)
		return fmt.Errorf(errMsg)
	}

	ar.logger.Printf("[INFO] Successfully updated project schedule status: projectID=%s, onSchedule=%v", projectID, projectOnSchedule)
	return nil
}

func (ar *AnalyticsRepo) GetProjectAnalytics(ctx context.Context, projectID string) (bson.M, error) {
	_, span := ar.tracer.Start(ctx, "AnalyticsRepo.GetProjectAnalytics")
	defer span.End()

	ar.logger.Printf("[INFO] Retrieving project analytics for projectID=%s", projectID)

	collection := ar.cli.Database("analytics_db").Collection("analytics")

	var project bson.M
	err := collection.FindOne(ctx, bson.M{"project_id": projectID}).Decode(&project)
	if err != nil {
		errMsg := fmt.Sprintf(errFetchingProjectFormat, projectID, err)
		span.RecordError(err)
		ar.logger.Printf(errGeneric, errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	ar.logger.Printf("[INFO] Successfully retrieved project analytics for projectID=%s", projectID)
	return project, nil
}
