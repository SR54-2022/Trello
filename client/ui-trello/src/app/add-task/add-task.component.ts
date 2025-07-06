import {Component, Input, OnInit, ViewChild} from '@angular/core';
import {FormBuilder, FormGroup, Validators} from "@angular/forms";
import {ActivatedRoute} from "@angular/router";
import {Task, TaskStatus} from "../models/task";
import {TaskService} from "../services/task.service";
import {HttpClient} from "@angular/common/http";
import {ToastrService} from "ngx-toastr";
import {AccountService} from "../services/account.service";
import {firstValueFrom} from "rxjs";
import {Project} from "../models/project.model";
import {UserResponse} from "../member-addition/member-addition.component";
import {ProjectServiceService} from "../services/project-service.service";
import {GraphEditorComponent} from "../graph-editor/graph-editor.component";
interface User {
  id: string;
  email: string;
}

@Component({
  selector: 'app-add-task',
  templateUrl: './add-task.component.html',
  styleUrls: ['./add-task.component.css']
})
export class AddTaskComponent implements OnInit {
  @ViewChild(GraphEditorComponent) graphEditor!: GraphEditorComponent;
  taskForm!: FormGroup;
  @Input()  projectId!: string;
  tasks: Task[] = [];
  tempStatusMap: { [taskId: string]: TaskStatus } = {};
  isManager: boolean = false;
  allUsers: User[] = [];
  filteredUsers: { [taskId: string]: User[] } = {};
  searchTerm: string = '';
  taskMembers: { [taskId: string]: User[] } = {};


  constructor(
    private fb: FormBuilder,
    private route: ActivatedRoute,
    private taskService: TaskService,
    private http: HttpClient,
    private toastr: ToastrService,
    private accountService: AccountService,
    private projectService: ProjectServiceService
  ) {
  }

  ngOnInit(): void {
    this.route.params.subscribe(params => {
      this.projectId = params['projectId'];
    });

    this.isUserManager().then(isManager => {
      this.isManager = isManager;
    });
    this.taskForm = this.fb.group({
      taskTitle: ['', Validators.required],
      taskDescription: ['', Validators.required]
    });

    this.projectService.getProjectById(this.projectId).subscribe({
      next: (project: Project) => {
        console.log('Project:', project);
        const userIds = project.user_ids || [];

        if (userIds.length === 0) {
          console.warn('No userIds provided. Skipping fetch for user details.');
          this.fetchTasks(this.projectId); // Still fetch tasks regardless of user details
          if (this.graphEditor) {
            this.graphEditor.fetchData();
          }

          return;
        }

        fetch('/api/user-server/users/details', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ userIds: userIds })
        })
          .then(response => response.json())
          .then(data => {
            this.allUsers = data;
            this.fetchTasks(this.projectId);
          })
          .catch(error => {
            console.error('Error:', error);
          });
      },
      error: (err) => {
        console.error('Error retrieving project:', err);
      }
    });
  }

  filterUsers(taskId: string) {
    this.filteredUsers[taskId] = this.allUsers
      .filter(user => user.email.toLowerCase().includes(this.searchTerm.toLowerCase()))
      .filter(user => {
        return !this.taskMembers[taskId]?.some(member => member.id === user.id);
      });
  }


  fetchTasks(projectId: string) {
    this.tasks = [];
    this.http.get<Task[]>(`/api/task-server/tasks/${projectId}`)
      .subscribe({
        next: (response) => this.handleFetchResponse(response),
        error: (error) => this.handleFetchError(error)
      })
  }

  private handleFetchResponse(response: Task[]) {
    this.tasks = response.reverse();
    this.tasks.forEach(task => {
      this.updateTaskMembers(task);
      this.updateFilteredUsers(task);
    });
    console.log("Successfully fetching tasks.");
  }

  private handleFetchError(error: any) {
    console.error("Error fetching data ",error);
  }

  private updateTaskMembers(task: Task) {
    this.tempStatusMap[task.id] = task.status as TaskStatus;
    this.taskMembers[task.id] = task.user_ids.map(userId =>
      this.findUserById(userId)).filter(user => user !== undefined) as User[];

  }

  private updateFilteredUsers(task: Task) {
    this.filteredUsers[task.id] = this.allUsers.filter(user =>
      !this.taskMembers[task.id].some(member => member.id === user.id)
    );
  }

  private findUserById(userId: string): User | undefined {
    return this.allUsers.find(user => user.id == userId);
  }

  onSubmit(): void {
    if (this.taskForm.valid) {
      const taskData: Task = new Task(
        '',
        this.projectId,
        this.taskForm.value.taskTitle,
        this.taskForm.value.taskDescription,
        'Pending',
        new Date(),
        new Date(),
        [],
        [],
        false
      );

      this.taskService.addTask(taskData).subscribe({
        next: (response) => {
          console.log('Task created successfully');
          this.fetchTasks(this.projectId);
          this.toastr.success("Task successfully created");
          console.log("The task is created: " + JSON.stringify(response.task))
          this.createWorkflowTask(response.task);
          this.taskForm.reset();
        },
        error: (error) => {
          console.error('Error creating task:', error);
          this.toastr.error(error || error.err() || "Something went wrong");
        }
      });

      console.log('Task Created:', taskData);
      console.log('Project ID:', this.projectId);

    }
  }



  getTaskDependencies(task: Task): Task[] {
    return this.tasks.filter(t => task.dependencies.includes(t.id));
  }


  async changeTaskStatus(task: Task): Promise<void> {
    const dependencies = this.getTaskDependencies(task);

    const hasPendingDependencies = dependencies.some(dep => dep.status === 'Pending');

    if (hasPendingDependencies) {
      this.toastr.warning("Cannot change status: One or more dependencies are still pending.");
      this.tempStatusMap[task.id] = task.status;
      return;
    }
    const isUserAssigned = await this.checkIfUserIsAssigned(task);
    if (!isUserAssigned) {
      this.toastr.warning("Cannot change status: You are not assigned to this task.");
      this.tempStatusMap[task.id] = task.status;
      return;
    }


    const updatedStatus = this.tempStatusMap[task.id];
    task.status = updatedStatus;

    this.taskService.updateTaskStatus(task).subscribe({
      next: () => {
        console.log('Task status updated to:', task.status);
        this.fetchTasks(this.projectId);
        this.toastr.success("Task status successfully updated");
      },
      error: (error) => {
        console.error('Error updating task status:', error);
        this.toastr.error(error || error.err() || "There has been a problem");
      }
    });
  }

  async checkIfUserIsAssigned(task: Task): Promise<boolean> {
    try {
      const value = await firstValueFrom(this.taskService.checkIfUserInTask(task));
      return value;
    } catch (error) {
      console.error('Error checking if user is assigned', error);
      return false;
    }
  }

  async isUserManager(): Promise<boolean> {
    try {
      const response = await firstValueFrom(this.taskService.checkIfUserIsManager(this.projectId));
      return response;
    } catch (error) {
      console.error("Error checking if user is manager", error);
      return false;
    }
  }

  addUserToTask(task: Task, user: User): void {
    if (!this.taskMembers[task.id]) {
      this.taskMembers[task.id] = [];
    }
    this.taskMembers[task.id].push(user);

    this.updateTaskMember(task.id, 'add', user.id);
    this.filterUsers(task.id)

  }

  removeUserFromTask(task: Task, user: User): void {
    const assignedMembers = this.taskMembers[task.id] || [];

    if (task.status === 'In Progress' && assignedMembers.length === 1) {
      this.toastr.warning("Cannot remove the last assigned person from an in-progress task.");
      return;
    }

    const index = assignedMembers.findIndex(member => member.id === user.id);
    if (index !== -1) {
      assignedMembers.splice(index, 1);
      this.taskMembers[task.id] = assignedMembers;

      this.updateTaskMember(task.id, 'remove', user.id);

      this.filterUsers(task.id);
    }
  }



  updateTaskMember(taskId: string, action: 'add' | 'remove', userId: string): void {
    const url = `/api/task-server/tasks/${taskId}/members/${action}/${userId}`;

    this.http.post(url, {}).subscribe({
      next: () => {
        console.log(`User ${action}ed successfully`);
      },
      error: (error) => {
        console.error('Error updating task member:', error);
      }
    });
  }


  private createWorkflowTask(task: Task) {
    const url = `/api/workflow-server/workflow`;

    this.http.post(url, task).subscribe({
      next: () => {
        console.log(`Workflow created successfully: `);

      },
      error: (error) => {
        console.error('Error updating task member:', error);
      }
    });

  }


}
