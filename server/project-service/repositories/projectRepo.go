package repositories

import (
	"context"
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
	"project-service/model"
	"time"
)

type ProjectRepo struct {
	cli    *mongo.Client
	logger *log.Logger
	tracer trace.Tracer
}

const (
	invalidUser    = "invalid user ID: %v"
	invalidProject = "invalid project ID: %v"
	removeFail     = "failed to remove user from project: %v"
)

func New(ctx context.Context, logger *log.Logger, tracer trace.Tracer) (*ProjectRepo, error) {
	dburi := os.Getenv("MONGO_DB_URI")
	if dburi == "" {
		return nil, fmt.Errorf("MONGO_DB_URI environment variable is not set")
	}

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(dburi))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	logger.Println("Successfully connected to MongoDB")
	return &ProjectRepo{
		cli:    client,
		logger: logger,
		tracer: tracer,
	}, nil
}

func (pr *ProjectRepo) Disconnect(ctx context.Context) error {
	_, span := pr.tracer.Start(ctx, "ProjectRepo.Disconnect")
	defer span.End()
	err := pr.cli.Disconnect(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetStatus(codes.Ok, "Connection to MongoDB closed.")
	return nil
}

func (pr *ProjectRepo) Ping() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := pr.cli.Ping(ctx, readpref.Primary())
	if err != nil {
		pr.logger.Println(err)
	}

	databases, err := pr.cli.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		pr.logger.Println(err)
	}
	fmt.Println(databases)
}

func (pr *ProjectRepo) GetAll(ctx context.Context) (model.Projects, error) {
	ctx, span := pr.tracer.Start(ctx, "ProjectRepo.GetAll")
	defer span.End()
	patientsCollection := pr.getCollection()

	var projects model.Projects
	patientsCursor, err := patientsCollection.Find(ctx, bson.M{})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		pr.logger.Println(err)
		return nil, err
	}
	if err = patientsCursor.All(ctx, &projects); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		pr.logger.Println(err)
		return nil, err
	}
	span.SetStatus(codes.Ok, "Successfully got all projects")
	return projects, nil
}

func (pr *ProjectRepo) GetAllByManager(ctx context.Context, managerEmail string) (model.Projects, error) {
	ctx, span := pr.tracer.Start(ctx, "ProjectRepo.GetAllByManager")
	defer span.End()

	projectsCollection := pr.getCollection()

	var projects model.Projects
	filter := bson.M{"manager": managerEmail, "pending_deletion": false}
	projectsCursor, err := projectsCollection.Find(ctx, filter)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		pr.logger.Println(err)
		return nil, err
	}
	if err = projectsCursor.All(ctx, &projects); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		pr.logger.Println(err)
		return nil, err
	}
	span.SetStatus(codes.Ok, "Successfully got all by managers")
	return projects, nil
}

func (pr *ProjectRepo) GetAllByMember(ctx context.Context, userID string) (model.Projects, error) {
	ctx, span := pr.tracer.Start(ctx, "ProjectRepo.GetAllByMember")
	defer span.End()

	projectsCollection := pr.getCollection()

	var projects model.Projects

	objID, err := primitive.ObjectIDFromHex(userID)
	if err != nil {
		return nil, fmt.Errorf(invalidUser, err)
	}

	filter := bson.M{"user_ids": objID, "pending_deletion": false}
	projectsCursor, err := projectsCollection.Find(ctx, filter)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		pr.logger.Println(err)
		return nil, err
	}
	if err = projectsCursor.All(ctx, &projects); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		pr.logger.Println(err)
		return nil, err
	}
	span.SetStatus(codes.Ok, "Successfully got all")
	return projects, nil
}

func (pr *ProjectRepo) GetById(ctx context.Context, id string) (*model.Project, error) {
	ctx, span := pr.tracer.Start(ctx, "ProjectRepo.GetById")
	defer span.End()
	patientsCollection := pr.getCollection()

	var patient model.Project
	objID, _ := primitive.ObjectIDFromHex(id)
	err := patientsCollection.FindOne(ctx, bson.M{"_id": objID}).Decode(&patient)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		pr.logger.Println(err)
		return nil, err
	}
	span.SetStatus(codes.Ok, "Successfully got all")
	return &patient, nil
}

func (pr *ProjectRepo) Insert(ctx context.Context, project *model.Project) (primitive.ObjectID, error) {
	ctx, span := pr.tracer.Start(ctx, "ProjectRepo.Insert")
	defer span.End()
	projectsCollection := pr.getCollection()

	if project.UserIDs == nil {
		project.UserIDs = []string{}
	}

	result, err := projectsCollection.InsertOne(ctx, &project)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		pr.logger.Println(err)
		return primitive.NilObjectID, err
	}

	pr.logger.Printf("Documents ID: %v\n", result.InsertedID)
	span.SetStatus(codes.Ok, "Successfully inserted a project")
	return result.InsertedID.(primitive.ObjectID), nil
}

func (pr *ProjectRepo) getCollection() *mongo.Collection {
	projectDatabase := pr.cli.Database("mongoTrello")
	projectCollection := projectDatabase.Collection("projects")
	return projectCollection
}

func (pr *ProjectRepo) AddUsersToProject(ctx context.Context, projectId string, userIds []string) error {
	ctx, span := pr.tracer.Start(ctx, "ProjectRepo.AddUsersToProject")
	defer span.End()

	collection := pr.getCollection()

	objID, err := primitive.ObjectIDFromHex(projectId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf(invalidProject, err)
	}

	var objIDs []primitive.ObjectID
	for _, userId := range userIds {
		objID, err := primitive.ObjectIDFromHex(userId)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return fmt.Errorf(invalidUser, err)
		}
		objIDs = append(objIDs, objID)
	}

	_, err = collection.UpdateOne(
		ctx,
		bson.M{"_id": objID},

		bson.M{"$addToSet": bson.M{"user_ids": bson.M{"$each": objIDs}}},
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("failed to add users to project: %v", err)
	}
	span.SetStatus(codes.Ok, "Successfully added users to project")
	return nil
}

func (pr *ProjectRepo) RemoveUserFromProject(ctx context.Context, projectId string, userId string) error {
	ctx, span := pr.tracer.Start(ctx, "ProjectRepo.RemoveUserFromProject")
	defer span.End()
	collection := pr.getCollection()

	projectObjID, err := primitive.ObjectIDFromHex(projectId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf(invalidProject, err)
	}

	userObjID, err := primitive.ObjectIDFromHex(userId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf(invalidUser, err)
	}

	_, err = collection.UpdateOne(
		ctx,
		bson.M{"_id": projectObjID},
		bson.M{"$pull": bson.M{"user_ids": userObjID}},
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf(removeFail, err)
	}
	span.SetStatus(codes.Ok, "Successfully removed user from project")
	return nil
}

func (pr *ProjectRepo) DeleteProject(ctx context.Context, projectId string) error {
	ctx, span := pr.tracer.Start(ctx, "ProjectRepo.DeleteProject")
	defer span.End()

	collection := pr.getCollection()

	projectObjID, err := primitive.ObjectIDFromHex(projectId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf(invalidProject, err)
	}

	_, err = collection.DeleteOne(
		ctx,
		bson.M{"_id": projectObjID},
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf(removeFail, err)
	}
	span.SetStatus(codes.Ok, "Successfully removed project")

	return nil
}
func (pr *ProjectRepo) PendingDeletion(ctx context.Context, projectId string, toBeDeleted bool) error {
	ctx, span := pr.tracer.Start(ctx, "ProjectRepo.PendingDeletion")
	defer span.End()
	collection := pr.getCollection()

	projectObjID, err := primitive.ObjectIDFromHex(projectId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf(invalidProject, err)
	}

	_, err = collection.UpdateOne(
		ctx,
		bson.M{"_id": projectObjID},
		bson.M{"$set": bson.M{"pending_deletion": toBeDeleted}},
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf(removeFail, err)
	}
	span.SetStatus(codes.Ok, "Successfully removed pending deletion")
	return nil
}

func (pr *ProjectRepo) IsUserManagerOfProject(ctx context.Context, userId string, projectId string) (bool, error) {

	pr.logger.Println("Hit the repo method")

	ctx, span := pr.tracer.Start(ctx, "ProjectRepo.IsUserManagerOfProject")
	defer span.End()

	patientsCollection := pr.getCollection()

	var project model.Project
	objID, _ := primitive.ObjectIDFromHex(projectId)
	err := patientsCollection.FindOne(ctx, bson.M{"_id": objID}).Decode(&project)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		pr.logger.Println(err)
	}

	userObjID, err := primitive.ObjectIDFromHex(userId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return false, fmt.Errorf(invalidUser, err)
	}

	managerObjID, err := primitive.ObjectIDFromHex(project.Manager)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return false, fmt.Errorf("invalid manager ID in project: %v", err)
	}
	span.SetStatus(codes.Ok, "Successful function")
	return managerObjID == userObjID, nil
}
