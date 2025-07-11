package repository

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"io/ioutil"
	"log"
	"main.go/customLogger"
	"main.go/model"
	"net/http"
	"os"
	"time"
)

type WorkflowRepo struct {
	driver     neo4j.DriverWithContext
	logger     *log.Logger
	custLogger *customLogger.Logger
	tracer     trace.Tracer
}

const (
	failErr       = "failed to execute update query: %w"
	noRecords     = "no records updated"
	successUpdate = "Successfully updated %d workflows for project %s\\n"
	noWorkflows   = "No workflows updated for project %s\n"
)

func New(logger *log.Logger, custLogger *customLogger.Logger, tracer trace.Tracer) (*WorkflowRepo, error) {
	uri := os.Getenv("NEO4J_DB")
	user := os.Getenv("NEO4J_USERNAME")
	pass := os.Getenv("NEO4J_PASS")
	auth := neo4j.BasicAuth(user, pass, "")

	driver, err := neo4j.NewDriverWithContext(uri, auth)
	if err != nil {
		logger.Panic(err)
		return nil, err
	}

	return &WorkflowRepo{
		driver:     driver,
		logger:     logger,
		custLogger: custLogger,
		tracer:     tracer,
	}, nil
}

func (wf *WorkflowRepo) CheckConnection() {
	ctx := context.Background()
	err := wf.driver.VerifyConnectivity(ctx)
	if err != nil {
		wf.logger.Panic(err)
		return
	}
	wf.logger.Printf(`Neo4J server address: %s`, wf.driver.Target().Host)
}

func (wf *WorkflowRepo) CloseDriverConnection(ctx context.Context) {
	wf.driver.Close(ctx)
}

func (wf *WorkflowRepo) GetAllNodesWithTask(ctx context.Context, limit int) (*model.TaskGraph, error) {
	ctx, span := wf.tracer.Start(ctx, "WorkflowRepo.GetAllNodesWithTask")
	defer span.End()
	session := wf.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	movieResults, err := session.ExecuteRead(ctx,
		func(transaction neo4j.ManagedTransaction) (any, error) {
			result, err := transaction.Run(ctx,
				`MATCH (task:Task)
				RETURN task.id as id, task.project_id as project_id, task.name as name, task.description as description, task.status as status, task.created_at as created_at, task.updated_at = updated_at, task.dependencies as dependencies, task.blocked as blocked
				LIMIT $limit`,
				map[string]any{"limit": limit})
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}

			var tasks model.TaskGraphs
			for result.Next(ctx) {
				record := result.Record()
				//id, _ := record.Get("id")
				projectId, _ := record.Get("project_id")
				name, _ := record.Get("name")
				description, _ := record.Get("description")
				status, _ := record.Get("status")
				createdAt, _ := record.Get("created_at")
				updatedAt, _ := record.Get("updated_at")
				blocked, _ := record.Get("blocked")
				tasks = append(tasks, &model.TaskGraph{
					//ID:          id.(string),
					ProjectID:   projectId.(string),
					Name:        name.(string),
					Description: description.(string),
					Status:      status.(model.TaskStatus),
					CreatedAt:   createdAt.(time.Time),
					UpdatedAt:   updatedAt.(time.Time),
					Blocked:     blocked.(bool),
				})
			}
			return tasks, nil

		})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		wf.logger.Println("Error querying search:", err)
		return nil, err
	}
	span.SetStatus(codes.Ok, "")
	return movieResults.(*model.TaskGraph), nil
}

func (wf *WorkflowRepo) PostTask(ctx context.Context, task *model.TaskGraph) error {
	ctx, span := wf.tracer.Start(ctx, "WorkflowRepo.PostTask")
	defer span.End()
	session := wf.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	savedPerson, err := session.ExecuteWrite(ctx,
		func(transaction neo4j.ManagedTransaction) (any, error) {
			result, err := transaction.Run(ctx,
				"CREATE (p:Task) SET p.id = $id, p.projectId = $projectId, p.name = $name, p.description = $description, p.status = $status, p.created_at = $created_at, p.updated_at = $updated_at, p.user_ids = $user_ids, p.dependencies = $dependencies, p.blocked = $blocked, p.pending_deletion = $pending_deletion  RETURN p.name + ', from node ' + id(p)",
				map[string]any{"id": task.ID, "projectId": task.ProjectID, "name": task.Name, "description": task.Description, "status": task.Status, "created_at": task.CreatedAt, "updated_at": task.UpdatedAt, "user_ids": task.UserIds, "dependencies": task.Dependencies, "blocked": task.Blocked, "pending_deletion": task.PendingDeletion})
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}

			wf.logger.Printf("Query parameters: %+v\n", map[string]any{
				"id":               task.ID,
				"project_id":       task.ProjectID,
				"name":             task.Name,
				"description":      task.Description,
				"status":           task.Status,
				"created_at":       task.CreatedAt,
				"updated_at":       task.UpdatedAt,
				"user_ids":         task.UserIds,
				"dependencies":     task.Dependencies,
				"blocked":          task.Blocked,
				"pending_deletion": task.PendingDeletion,
			})
			if result.Next(ctx) {
				return result.Record().Values[0], nil
			}

			return nil, result.Err()
		})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		wf.logger.Println("Error inserting Person:", err)
		return err
	}
	span.SetStatus(codes.Ok, "")
	wf.logger.Println(savedPerson.(string))
	return nil
}

func (wf *WorkflowRepo) GetOne(ctx context.Context, taskID int) (*model.TaskGraph, error) {
	ctx, span := wf.tracer.Start(ctx, "WorkflowRepo.GetOne")
	defer span.End()
	session := wf.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	task, err := session.ExecuteRead(ctx, func(transaction neo4j.ManagedTransaction) (any, error) {
		t, err := handleQueryTask(taskID, transaction, ctx, span)
		if err != nil {
			return nil, err
		}
		return t.(*model.TaskGraph), nil

	})

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		wf.logger.Println("Error retrieving task:", err)
		return nil, err
	}
	span.SetStatus(codes.Ok, "")
	return task.(*model.TaskGraph), nil
}

func handleQueryTask(taskID int, transaction neo4j.ManagedTransaction, ctx context.Context, span trace.Span) (any, error) {
	query := `
			MATCH (t:Task {id: $id})
			OPTIONAL MATCH (t)-[:DEPENDS_ON]->(d:Task)
			RETURN t.id AS id, t.projectId AS projectId, t.name AS name, 
			       t.description AS description, t.status AS status, 
			       t.created_at AS created_at, 
			       t.updated_at AS updated_at, 
				   t.user_ids AS user_ids,
			       collect(d.id) AS dependencies, 
			       t.blocked AS blocked
		`
	params := map[string]any{"id": taskID}
	result, err := transaction.Run(ctx, query, params)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	if result.Next(ctx) {
		return handleResultNext(result)
	}

	return nil, result.Err()
}

func handleResultNext(result neo4j.ResultWithContext) (any, error) {
	record := result.Record()
	var dependencies []string
	if deps, ok := record.Get("dependencies"); ok {
		dependencies = getDependencies(deps)
	}

	var userIDs []string
	if ids, ok := record.Get("user_ids"); ok && ids != nil {
		userIDs = getUserIDs(ids)
	}
	return &model.TaskGraph{
		ID:           record.Values[0].(string),
		ProjectID:    record.Values[1].(string),
		Name:         record.Values[2].(string),
		Description:  record.Values[3].(string),
		Status:       model.TaskStatus(record.Values[4].(string)),
		CreatedAt:    record.Values[5].(time.Time),
		UpdatedAt:    record.Values[6].(time.Time),
		UserIds:      userIDs,
		Dependencies: dependencies,
		Blocked:      record.Values[8].(bool),
	}, nil
}

func getDependencies(deps any) []string {
	var dependencies []string
	for _, dep := range deps.([]any) {
		dependencies = append(dependencies, dep.(string))
	}

	return dependencies
}

func getUserIDs(ids any) []string {
	var userIds []string
	for _, id := range ids.([]any) {
		userIds = append(userIds, id.(string))
	}
	return userIds
}

func (wf *WorkflowRepo) AddDependency(ctx context.Context, taskID string, dependencyID string) error {
	ctx, span := wf.tracer.Start(ctx, "WorkflowRepo.AddDependency")
	defer span.End()
	session := wf.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(transaction neo4j.ManagedTransaction) (any, error) {

		query := `
        MATCH (t:Task {id: $taskID}), (d:Task {id: $dependencyID})
        OPTIONAL MATCH path1 = (d)-[:DEPENDS_ON*]->(t)
        OPTIONAL MATCH path2 = (t)-[:DEPENDS_ON*]->(d)
        OPTIONAL MATCH (t)-[r:DEPENDS_ON]->(d)
        RETURN 
            path1 IS NOT NULL OR path2 IS NOT NULL AS hasCycle,
            r IS NOT NULL AS hasExistingRelationship
    `

		params := map[string]any{"taskID": taskID, "dependencyID": dependencyID}
		result, err := transaction.Run(ctx, query, params)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

		if result.Next(ctx) {
			record := result.Record()
			hasCycle, _ := record.Get("hasCycle")
			hasExistingRelationship, _ := record.Get("hasExistingRelationship")
			wf.logger.Println("Result from hasExistingRelationship:", hasExistingRelationship)
			wf.logger.Println("Result from hasCycle:", hasCycle)

			wf.logger.Println("Result from hasExistingRelationship.bool:", hasExistingRelationship.(bool))
			wf.logger.Println("Result from hasCycle.bool:", hasCycle.(bool))

			if hasExistingRelationship.(bool) {
				span.RecordError(errors.New("a dependency relationship already exists between these tasks"))
				return nil, errors.New("a dependency relationship already exists between these tasks")
			}
			if hasCycle.(bool) {
				span.RecordError(errors.New("adding this dependency would create a cycle"))
				return nil, errors.New("adding this dependency would create a cycle")
			}
		}

		updateQuery := `
			MATCH (t1:Task {id: $taskID}), (t2:Task {id: $dependencyID})
			CREATE (t1)-[:DEPENDS_ON {created_at: datetime()}]->(t2);
		`
		updateParams := map[string]any{"taskID": taskID, "dependencyID": dependencyID}
		_, err = transaction.Run(ctx, updateQuery, updateParams)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

		return nil, nil

	})

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		wf.logger.Println("Error adding dependency:", err)
		return err
	}
	span.SetStatus(codes.Ok, "")
	return nil
}

func (wf *WorkflowRepo) GetTaskGraph(ctx context.Context, projectID string) (map[string]any, error) {
	ctx, span := wf.tracer.Start(ctx, "WorkflowRepo.GetTaskGraph")
	defer span.End()
	session := wf.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		return handleQuery(tx, projectID, ctx, span, wf.logger)
	})

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		wf.logger.Println("Error querying task graph:", err)
		return nil, err
	}

	span.SetStatus(codes.Ok, "Successfully retrieved task graph")
	return result.(map[string]any), nil

}

func handleQuery(tx neo4j.ManagedTransaction, projectID string, ctx context.Context, span trace.Span, logger *log.Logger) (any, error) {
	query := `
            MATCH (task:Task {projectId: $projectID})
			OPTIONAL MATCH (task)-[:DEPENDS_ON]->(dep:Task)
			RETURN 
			    task.id AS id,
    			task.name AS name,
    			task.description AS description,
    			task.status AS status,
    			task.blocked AS blocked,
    			task.user_ids AS user_ids,
    			task.created_at AS created_at,
    			task.updated_at AS updated_at,
    			collect(dep.id) AS dependencies

        `
	params := map[string]any{"projectID": projectID}
	res, err := tx.Run(ctx, query, params)
	if err != nil {
		logger.Println("Query execution failed:", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	return handleResNext(res, ctx, logger, span)
}

func handleResNext(res neo4j.ResultWithContext, ctx context.Context, logger *log.Logger, span trace.Span) (any, error) {
	graph := map[string]any{"nodes": []map[string]any{}, "edges": []map[string]string{}}
	nodesMap := make(map[string]map[string]any)
	edgesSet := make(map[string]bool)
	logger.Println("graph:", graph)
	for res.Next(ctx) {
		record := res.Record()
		taskID, _ := record.Get("id")
		taskName, _ := record.Get("name")
		taskDescription, _ := record.Get("description")
		dependencies, _ := record.Get("dependencies")
		taskBlocked, _ := record.Get("blocked")

		taskIDStr, ok := taskID.(string)
		if !ok {
			logger.Println("Invalid task ID:", taskID)
			continue
		}
		taskCompleteStr, _ := getTaskStatusFromService(taskIDStr)
		taskNameStr, ok := taskName.(string)
		if !ok {
			logger.Println("Invalid task name:", taskName)
			continue
		}
		taskDescriptionStr, ok := taskDescription.(string)
		if !ok {
			logger.Println("Invalid task description:", taskDescription)
			continue
		}
		taskComplete := taskCompleteStr == ("Completed")
		logger.Println("taskCompleteStr")
		logger.Println(taskCompleteStr)
		logger.Println("taskComplete")
		logger.Println(taskComplete)

		logger.Println("--")

		handleTaskDependencies(taskIDStr, taskNameStr, taskDescriptionStr, taskBlocked, taskComplete, dependencies, &nodesMap, &edgesSet, &graph, logger)
	}

	return handleSlice(graph, logger, span, res, nodesMap)

}

func handleTaskDependencies(
	taskIDStr string,
	taskNameStr string,
	taskDescriptionStr string,
	taskBlocked any,
	taskComplete bool,
	dependencies any,
	nodesMap *map[string]map[string]any,
	edgesSet *map[string]bool,
	graph *map[string]any,
	logger *log.Logger,
) {
	// Process dependencies
	dependenciesList, ok := dependencies.([]any)
	if !ok {
		logger.Println("Invalid dependencies format:", dependencies)
		return
	}

	// Add the task to nodesMap if it doesn't exist
	if _, exists := (*nodesMap)[taskIDStr]; !exists {
		(*nodesMap)[taskIDStr] = map[string]any{
			"id":          taskIDStr,
			"label":       taskNameStr,
			"description": taskDescriptionStr,
			"blocked":     taskBlocked,
			"isComplete":  taskComplete,
		}
	}

	// Process each dependency
	for _, dep := range dependenciesList {
		depID, ok := dep.(string)
		if !ok {
			logger.Println("Invalid dependency ID:", dep)
			continue
		}
		addEdge(taskIDStr, depID, edgesSet, graph)
	}
}

func addEdge(taskIDStr string, depID string, edgesSet *map[string]bool, graph *map[string]any) {
	edgeKey := taskIDStr + "->" + depID
	if !(*edgesSet)[edgeKey] {
		(*edgesSet)[edgeKey] = true
		edges, _ := (*graph)["edges"].([]map[string]string)
		(*graph)["edges"] = append(edges, map[string]string{
			"from": taskIDStr,
			"to":   depID,
		})
	}
}

func handleSlice(graph map[string]any, logger *log.Logger, span trace.Span, res neo4j.ResultWithContext, nodesMap map[string]map[string]any) (any, error) {
	nodesSlice := make([]map[string]any, 0, len(nodesMap))
	for _, node := range nodesMap {
		nodesSlice = append(nodesSlice, node)
	}
	graph["nodes"] = nodesSlice

	if err := res.Err(); err != nil {
		logger.Println("Error in query result:", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	return graph, nil
}

func getTaskStatusFromService(taskID string) (string, error) {

	url := fmt.Sprintf("https://task-server:8080/tasks/%s/status", taskID)

	client, err := createTLSClient()
	if err != nil {
		log.Printf("Error creating TLS client: %v\n", err)
		return "", fmt.Errorf("failed to create client: %w", err)
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to make new request to task-server: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to call task-server: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("task-server returned status %d", resp.StatusCode)
	}

	var statusResponse string
	if err := json.NewDecoder(resp.Body).Decode(&statusResponse); err != nil {
		return "", fmt.Errorf("failed to decode task-server response: %w", err)
	}

	return statusResponse, nil
}

func createTLSClient() (*http.Client, error) {
	caCert, err := ioutil.ReadFile("/app/cert.crt")
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		RootCAs: caCertPool,
	}

	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}

	client := &http.Client{
		Transport: transport,
	}

	return client, nil
}

func (w *WorkflowRepo) UpdateAllWorkflowByProjectId(projectID string, toDelete bool) error {

	query := `
		MATCH (t {projectId: $projectID})
		SET t.pending_deletion = $toDelete, 
		    t.updated_at = $updatedAt
		RETURN COUNT(t) AS updatedCount
	`

	ctx := context.Background()
	session := w.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	updatedAt := time.Now().Format(time.RFC3339)
	result, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		res, err := tx.Run(ctx, query, map[string]any{
			"toDelete":  toDelete,
			"projectID": projectID,
			"updatedAt": updatedAt,
		})
		if err != nil {
			return nil, fmt.Errorf(failErr, err)
		}

		if res.Next(ctx) {
			return res.Record().Values[0], nil
		}
		return nil, fmt.Errorf(noRecords)
	})
	if err != nil {
		return fmt.Errorf("failed to update workflows for project %s: %w", projectID, err)
	}
	updatedCount, ok := result.(int64)
	if ok && updatedCount > 0 {
		fmt.Printf(successUpdate, updatedCount, projectID)
	} else {
		fmt.Printf(noWorkflows, projectID)
	}

	return nil
}

func (w *WorkflowRepo) DeleteAllWorkflowByProjectId(projectID string) error {

	query := `
		MATCH (t {projectId: $projectID})
		DETACH DELETE t
		RETURN COUNT(t) AS deletedCount
	`

	ctx := context.Background()
	session := w.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	result, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		res, err := tx.Run(ctx, query, map[string]any{
			"projectID": projectID,
		})
		if err != nil {
			return nil, fmt.Errorf(failErr, err)
		}

		if res.Next(ctx) {
			return res.Record().Values[0], nil
		}
		return nil, fmt.Errorf(noRecords)
	})
	if err != nil {
		return fmt.Errorf("failed to update workflows for project %s: %w", projectID, err)
	}
	deletedCount, ok := result.(int64)
	if ok && deletedCount > 0 {
		fmt.Printf(successUpdate, deletedCount, projectID)
	} else {
		fmt.Printf(noWorkflows, projectID)
	}

	return nil
}

func (w *WorkflowRepo) BlockWorkflow(taskId string, blocked bool) error {
	query := ` MATCH (n {id: $taskId}) SET n.blocked = $blocked, n.updated_at = $updated_at RETURN COUNT(n) AS updatedCount `

	ctx := context.Background()
	session := w.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	updatedAt := time.Now().Format(time.RFC3339)
	result, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		res, err := tx.Run(ctx, query, map[string]any{
			"taskId":     taskId,
			"blocked":    blocked,
			"updated_at": updatedAt,
		})
		if err != nil {
			return nil, fmt.Errorf(failErr, err)
		}

		if res.Next(ctx) {
			return res.Record().Values[0], nil
		}
		return nil, fmt.Errorf(noRecords)
	})
	if err != nil {
		return fmt.Errorf("failed to update blocked flag in workflow with taskId %s: %w", taskId, err)
	}
	updatedCount, ok := result.(int64)
	if ok && updatedCount > 0 {
		fmt.Printf(successUpdate, updatedCount, taskId)
	} else {
		fmt.Printf(noWorkflows, taskId)
	}

	return nil
}
