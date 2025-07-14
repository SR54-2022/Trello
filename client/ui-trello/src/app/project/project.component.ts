import {Component, OnInit, ViewChild} from '@angular/core';
import {ProjectDetails} from "../models/projectDetails";
import {ProjectServiceService} from "../services/project-service.service";
import {ActivatedRoute, Router} from "@angular/router";
import {TaskDetails} from "../models/taskDetails";
import {Task, TaskStatus} from "../models/task";
import {TaskService} from "../services/task.service";

import {UserDetails} from "../models/userDetails";
import {HttpClient, HttpHeaders} from "@angular/common/http";
import {ToastrService} from "ngx-toastr";
import {TaskDocumentDetails} from "../models/taskDocumentDetails.model";

import { CdkDragDrop, transferArrayItem } from '@angular/cdk/drag-drop';
import {GraphEditorComponent} from "../graph-editor/graph-editor.component";

@Component({
  selector: 'app-project',
  templateUrl: './project.component.html',
  styleUrl: './project.component.css'
})
export class ProjectComponent implements OnInit {
  @ViewChild(GraphEditorComponent) graphEditor!: GraphEditorComponent;

  project: ProjectDetails | null = null;
  projectId: string | null = null;

  pendingTasks: TaskDetails[] = [];
  inProgressTasks: TaskDetails[] = [];
  completedTasks: TaskDetails[] = [];

  selectedTask: TaskDetails | null = null;
  isDragging = false;
  isUserInTask: boolean = false;

  showAddTaskModal: boolean = false;
  searchTerm: string = '';
  private allUsers!: UserDetails[];
  filteredUsers: { [taskId: string]: UserDetails[] } = {};
  taskMembers: { [taskId: string]: UserDetails[] } = {};
  tasks: TaskDetails[] = [];


  taskDocumentDetails: TaskDocumentDetails[] = [];



  constructor(private readonly projectService: ProjectServiceService,private readonly router: Router ,private readonly route: ActivatedRoute,private readonly taskService: TaskService, private readonly http: HttpClient, private readonly toastr: ToastrService) {}

  ngOnInit(): void {
    this.route.paramMap.subscribe(params => {

      const projectId = params.get('projectId');


      if (projectId) {
        this.projectId = projectId;
        this.loadProjectDetails(projectId);

      } else {

        this.projectId = '';
        console.error('Project ID nije prisutan u URL-u!');
      }
    });
  }

  isManager(): boolean {
    const role = localStorage.getItem('role');
    return role === 'manager';
  }

  loadProjectDetails(projectId: string): void {
    this.projectService.getProjectDetailsById(projectId).subscribe({
      next: (data: ProjectDetails) => {
        this.project = data;
        this.tasks = this.project.tasks;

        console.log('Project:', this.project);

        const userIds = this.project.user_ids;
        if (!Array.isArray(userIds) || userIds.length === 0) {
          console.warn('No userIds provided, skipping fetch.');
          this.organizeTasksByStatus(this.project.tasks);
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
            console.log('User data:', data);
            this.allUsers = data;
            if (this.graphEditor) {
              this.graphEditor.fetchData();
            }
          })
          .catch(error => {
            console.error('Error fetching user details:', error);
          });

        if (this.project && this.project.tasks) {
          this.organizeTasksByStatus(this.project.tasks);
        }
      },
      error: (err) => {
        console.error('Error loading project details:', err);
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

  organizeTasksByStatus(tasks: TaskDetails[]): void {
    this.pendingTasks = tasks.filter(task => task.status === 'Pending');
    this.inProgressTasks = tasks.filter(task => task.status === 'In Progress');
    this.completedTasks = tasks.filter(task => task.status === 'Completed');
  }

  openTask(task: TaskDetails): void {

    if (!this.isDragging) {
      console.log(task);

      this.selectedTask = task;

      console.log(this.selectedTask.users);

      console.log(this.selectedTask.userIds);
      console.log(this.selectedTask.user_ids);

      this.taskMembers[task.id] = task.user_ids
        .map(userId => this.allUsers.find(user => user.id === userId))
        .filter((user): user is UserDetails => user !== undefined);

      this.filteredUsers[task.id] = this.allUsers.filter(user =>
        !this.taskMembers[task.id].some(member => member.id === user.id)
      );

      this.checkIfUserInTask();
      this.getTaskDocumentsForTask();
    }

  }




  showAddMemberModal = false;

  openMemberAdditionModal(projectId: string | null): void {
    if (projectId) {
      this.projectId = projectId;
      this.showAddMemberModal = true;
    } else {
      console.error('Project ID is null');
    }
  }


  closeMemberAdditionModal() {
    this.showAddMemberModal = false;
  }

  closeTask(): void {
    this.selectedTask = null;
    console.log(this.selectedTask);

  }

  showDropdown: boolean = false;

  toggleDropdown(): void {
    this.showDropdown = !this.showDropdown;
  }



  changeStatus(newStatus: TaskStatus): void {
    if (this.selectedTask) {
      const dependencies = this.getAllTaskDependencies(this.selectedTask);
      console.log("All Dependencies:::" + JSON.stringify(dependencies));


      const taskId = this.selectedTask.id;
      this.projectService.updateTaskStatus(taskId, newStatus).subscribe({
        next: () => {
          this.selectedTask!.status = newStatus; // Update the status locally
          console.log(`Status successfully updated to: ${newStatus}`);
          this.refreshTaskLists(); // Refresh tasks based on the new status
          this.toastr.success("Successfully changed the status.");
        },
        error: (err) => {
          console.error('Failed to update task status:', err);
          this.toastr.warning("You can't change the status at this time.");
        },
      });
    }
  }


  getAllTaskDependencies(task: TaskDetails): TaskDetails[] {
    console.log("Fetching dependencies for task: " + JSON.stringify(task));


    const dependencies: TaskDetails[] = this.tasks.filter(t => task.dependencies.includes(t.id));
    console.log("Direct Dependencies: " + JSON.stringify(dependencies));


    let allDependencies: TaskDetails[] = [...dependencies];
    dependencies.forEach(dep => {
      const subDependencies = this.getAllTaskDependencies(dep);
      allDependencies = [...allDependencies, ...subDependencies];
    });

    console.log("All Dependencies including nested: " + JSON.stringify(allDependencies));

    return allDependencies;
  }

  refreshTaskLists(): void {
    if (this.project && this.project.tasks) {
      this.organizeTasksByStatus(this.project.tasks);
    }
  }

  checkIfUserInTask(): void {
    if (this.selectedTask) {
      const task: Task = {
        id: this.selectedTask.id,
        projectId: this.selectedTask.projectId,
        name: this.selectedTask.name,
        description: this.selectedTask.description,
        status: this.selectedTask.status,
        createdAt: this.selectedTask.createdAt,
        updatedAt: this.selectedTask.updatedAt,
        user_ids: this.selectedTask.user_ids,
        dependencies: this.selectedTask.dependencies,
        blocked: this.selectedTask.blocked
      };
      console.log(task);
      // Poziv servisa sa konvertovanim objektom
      this.taskService.checkIfUserInTask(task).subscribe({
        next: (response: boolean) => {
          this.isUserInTask = response;
          console.log(this.isUserInTask);
          console.log('User is in task:', response);
        },
        error: err => {
          console.error('Error checking user in task:', err);
        }
      });
    } else {
      console.warn('No task selected for user check.');
      this.isUserInTask = false;
    }
  }


  openAddTaskModal (projectId: string | null): void{
    if (projectId) {
      this.projectId = projectId;
      this.showAddTaskModal = true;
    } else {
      console.error('Project ID is null');
    }

  }




  closeTaskAdditionModal(): void {
    this.showAddTaskModal = false;
    if(this.projectId != null){
      this.loadProjectDetails(this.projectId);
    }
  }


  addUserToTask(selectedTask: TaskDetails, user: UserDetails) {
    console.log(user)
    if (!this.taskMembers[selectedTask.id]) {
      this.taskMembers[selectedTask.id] = [];
    }
    if (this.isUserInTask) {
      this.toastr.warning("You can't add this user");
      return;
    }
    this.taskMembers[selectedTask.id].push(user);

    this.updateTaskMember(selectedTask.id, 'add', user.id);
    this.filterUsers(selectedTask.id)
  }

  addDependencyToTask(selectedTaskId: string, dependencyId: string) {
    if (this.selectedTask == null) {
      console.error("No selected task. Cannot add dependency.");
      return;
    }

    if (!Array.isArray(this.selectedTask.dependencies)) {
      this.selectedTask.dependencies = [];
    }

    this.selectedTask.dependencies.push(dependencyId); // Safe to push now
    console.log("Updated selected task:", this.selectedTask);

    this.addDependency(selectedTaskId, dependencyId);
  }



  removeUserFromTask(selectedTask: TaskDetails, member: UserDetails) {
    const assignedMembers = this.taskMembers[selectedTask.id] || [];

    if (selectedTask.status === 'In Progress' && assignedMembers.length === 1) {
      this.toastr.warning("Cannot remove the last assigned person from an in-progress task.");
      return;
    }

    const index = assignedMembers.findIndex(member => member.id === member.id);
    if (index !== -1) {
      assignedMembers.splice(index, 1);
      this.taskMembers[selectedTask.id] = assignedMembers;

      this.updateTaskMember(selectedTask.id, 'remove', member.id);

      this.filterUsers(selectedTask.id);
    }
  }


  private updateTaskMember(id: string, action: string, userId: string) {
    const url = `/api/task-server/tasks/${id}/members/${action}/${userId}`;

    this.http.post(url, {}).subscribe({
      next: () => {
        console.log(`User ${action}ed successfully`);
        if (this.projectId) {
          this.loadProjectDetails(this.projectId);
        }
      },
      error: (error) => {
        console.error('Error updating task member:', error);
      }
    });

  }

  private addDependency(id: string, dependecyId: string) {
    const url = `/api/workflow-server/workflow/${id}/add/${dependecyId}`;

    if(id == dependecyId){
      this.toastr.error("Task cannot be dependent on itself.");
      return;
    }

    console.log("task id: ", id);
    console.log("dependecy id: ", dependecyId);
    console.log("URL id: ", url);
    const headers = new HttpHeaders({ 'Content-Type': 'application/json' });

    this.http.post(url, {}, { headers }).subscribe({
      next: () => {
        console.log(`User added dependency successfully: ${dependecyId}`);
        if (this.projectId) {
          this.loadProjectDetails(this.projectId);
          this.toastr.success("Succesfully created connection between tasks");
        }
      },
      error: (error) => {
        console.error('Error making task dependecy:', error);
        this.toastr.error("This dependecy will cause cycles.");

      }
    });
  }


  private craeteWorkflowTask(task: Task) {
    const url = `/api/workflow-server/workflow`;

    // TODO: find out why it always returns 201 when it doesnt create dependency
    this.http.post(url, task).subscribe({
      next: () => {
        console.log(`Workflow created successfully: `);
        if (this.projectId) {
          this.loadProjectDetails(this.projectId);
        }
      },
      error: (error) => {
        console.error('Error updating task member:', error);
      }
    });

  }

  navigateToHistory(projectId: string | null ) {
    this.router.navigate(['/history/' + projectId]);
  }

  navigateToAnalytics(projectId: string | null ) {
    this.router.navigate(['/analytics/' + projectId]);
  }



  selectedFile: File | null = null;

  onFileSelected(event: Event): void {


    const input = event.target as HTMLInputElement;

    if (input.files && input.files.length > 0) {
      this.selectedFile = input.files[0];
      console.log('Fajl je učitan:', this.selectedFile.name);
    }
  }

  sendFile(): void {
    if (!this.selectedFile) {
      console.error('Nije izabran fajl za slanje.');
      return;
    }
    const taskId = this.selectedTask?.id!;

    this.taskService.uploadTaskDocument(taskId, this.selectedFile).subscribe({
      next: (response) => {
        console.log('File sent successfully:', response);

        this.selectedFile = null;
      },
      error: err => {
        console.error('Error while sending the file:', err);

        this.selectedFile = null;
        this.getTaskDocumentsForTask();
      }
    })


  }

  getTaskDocumentsForTask(): void {
    if (this.selectedTask) {
      this.taskService.getAllDocumentsForThisTask(this.selectedTask.id).subscribe({
        next: (response: TaskDocumentDetails[]) => {
          this.taskDocumentDetails = response;
          console.log('Task documents:', this.taskDocumentDetails);
        },
        error: (error) => {
          console.error('Error fetching task documents:', error);
        }
      })
    } else {
      console.warn('No task ID selected for fetching documents.');
    }
  }

  downloadFile1(doc: TaskDocumentDetails): void {
    const url = '';
    this.http.get(url, { responseType: 'blob' }).subscribe((blob) => {
      const a = document.createElement('a');
      const objectUrl = URL.createObjectURL(blob);
      a.href = objectUrl;
      a.download = doc.fileName;
      a.click();
      URL.revokeObjectURL(objectUrl);
    });
  }

  downloadFile(doc: TaskDocumentDetails): void {
    const url = `/api/task-server/tasks/download/${doc.id}`

    this.http.get(url, { responseType: 'blob' }).subscribe({
      next: (blob) => {
        const a = document.createElement('a');
        const objectUrl = URL.createObjectURL(blob);
        a.href = objectUrl;
        a.download = doc.fileName;
        a.click();
        URL.revokeObjectURL(objectUrl);
      },
      error: (err) => {
        console.error('Failed to download file', err);
      },
    });
  }

  getTaskNameById(depId: string): string {
    const name = this.tasks.find(t => t.id === depId)
    if (!name) {
      return ""
    }
    return name.name;

  }

  getAllTaskDependenciesRecursive(task: TaskDetails): TaskDetails[] {
    const dependencies = this.tasks.filter(t => task.dependencies.includes(t.id));
    const allDependencies = [...dependencies];

    dependencies.forEach(dep => {
      allDependencies.push(...this.getAllTaskDependenciesRecursive(dep));
    });

    return allDependencies;
  }


  onDragStart() {
    this.isDragging = true;
  }

  onDragEnd(): void {
    setTimeout(() => {
      this.isDragging = false;
    }, 0);
  }

  onPointerDown(event: PointerEvent): void {
    if (this.isDragging) {
      event.preventDefault();
    }
  }

  onDrop(event: CdkDragDrop<any[]>) { //TODO: fix this to not openTasks
    if (event.previousContainer !== event.container) {
      console.log('Dropped item:', event.item.data);
      console.log('Previous container:', event.previousContainer.id);
      console.log('Current container:', event.container.id);
      console.log('Current container:', event.container.id);
      const draggedTask = event.item.data;
      this.selectedTask = draggedTask
      let newStatus: TaskStatus = <"Pending" | "In Progress" | "Completed">"";
      if (event.container.id === 'cdk-drop-list-1') {
        newStatus = 'Pending';
      } else if (event.container.id === 'cdk-drop-list-2') {
        newStatus = 'In Progress';
      } else if (event.container.id === 'cdk-drop-list-3') {
        newStatus = 'Completed';
      }
      console.log('New status:', newStatus);


      if (newStatus && draggedTask.status !== newStatus) {
        if(draggedTask.blocked){
          this.toastr.error("Cannot change status: This Task is blocked.");
          return;
        }

        this.changeStatus(newStatus);
        if(this.projectId != null){
            this.loadProjectDetails(this.projectId);
        }
      }


      transferArrayItem(
        event.previousContainer.data,
        event.container.data,
        event.previousIndex,
        event.currentIndex
      );
    }
  }

  getAvailableDependencies(): TaskDetails[] {
    const allDependencies = this.getAllTaskDependenciesRecursive(this.selectedTask!);

    return this.tasks.filter(task =>
      task.id !== this.selectedTask!.id && !allDependencies.some(dep => dep.id === task.id)
    );
  }

  handleKeyDown(event: KeyboardEvent, doc: any) {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      this.downloadFile(doc);
    }
  }
}

