# Trello

Introducing a project management application, inspired by Trello, that allows users to organize tasks within projects. It supports user roles, workflows, task management, notifications, and project analytics.

---

## Architecture of the project

- **Frontend**: Angular 17 (v17.3.x)
  - **Security**: ng-recaptcha, jwt, HttpOnly cookie
- **Backend**: Golang 1.22.2 - microservices (REST)
- **API Gateway**: Entry point for frontend communication (Nginx, using HTTPS with self-signed TLS certificate)
- **Databases**:
  - Document: MongoDB (users, projects, tasks, analytics from cqrs)
  - Wide-Column: Cassandra (notifications)
  - Graph: Neo4j (workflow visualization)
  - In-Memory/Cache: Redis (caching project activity)
  - Event Sourcing: EventStoreDB (storing all project activity as events)
- **Storage**:
  - HDFS (documents attached to tasks):
    - NameNode
    - DataNode
- **Config**:
  - Environment variables (.env)

---

Each microservice is containerized using **Docker** (27.5.1) and orchestrated via **Docker Compose** (v2.32.4-desktop.1).

Tracing implemented using Jaeger.

### User Roles

- **Unauthenticated User**: Can register and log in.
- **Manager**: Creates and manages projects, adds/removes members and tasks, creates workflows, has insight into project history.
- **Member**: Participates in projects and updates task statuses, attaches documents, receives notifications, has insight into project analytics.

---

## Features

### Authentication & Users
- Register/login as Manager or Member
- Delete user accounts (with validation on active participation)

### Projects & Tasks 
- Create and delete projects (Saga pattern for deletion)
- Add/remove members
- Create/edit/delete tasks
- Assign members to tasks
- Update task status ('Pending', 'In Progress', 'Done')
- Attach documents to tasks

### Workflow (Graph-based)
- Define task dependencies
- Block/unblock tasks based on prerequisites
- Visualize workflow as a graph

### Notifications (NATS)
- Notify members when:
  - Added/removed from project
  - Assigned/unassigned from task
  - Task status changes

### Analytics (CQRS + Event Sourcing)
- Total number of tasks
- Number of tasks by status
- Time spent per status
- Deadline tracking
- Member-task assignment mapping

---

## Security Controls

### Data Validation & Protection
- Input validation based on OWASP standards
- Prevention of SQL Injection, XSS
- Validations implemented per secure coding best practices

### Secure Communication
- HTTPS enforced between API Gateway, client and services

### Authentication & Access Control
- Account confirmation, password recovery, password change
- Passwordless login via magic link
- RBAC-based endpoint access control
- Frontend route protection

### Data Protection
- Data encrypted in transit and at rest
- Passwords hashed using bcrypt

### Logging
- Logs are optimized, minimal yet informative
- Standardized format JSON for easy parsing

---

Project is used for educational purposes only.

Collaborators:
- Miodrag Ugrica
- Stefan Milutinovic
- Dora Tot
- Gabriela Franjo


