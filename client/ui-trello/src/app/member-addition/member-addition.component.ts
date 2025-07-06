import {Component, Input, OnInit} from '@angular/core';
import {ProjectServiceService} from "../services/project-service.service";
import {ActivatedRoute, Router} from "@angular/router";
import {AccountService} from "../services/account.service";
import {Project} from "../models/project.model";

export interface UserResponse {
  id: string;
  email: string;
  first_name: string;
  last_name: string;
  password: string;
  role: string;
}

interface User {
  id: string;
  email: string;
}

@Component({
  selector: 'app-member-addition',
  templateUrl: './member-addition.component.html',
  styleUrls: ['./member-addition.component.css']
})
export class MemberAdditionComponent implements OnInit {
  allUsers: User[] = [];
  projectMembers: any[] = [];
  searchTerm: string = '';
  minMembers: number = 0;
  maxMembers: number = 0;
  filteredUsers: User[] = [];
  @Input() projectId: string = '';
  project: Project | null = null;
  managerId: string = "";

  constructor(
    private projectService: ProjectServiceService,
    private route: ActivatedRoute,
    private accountService: AccountService,
    private router: Router
  ) {}

  ngOnInit() {
    this.managerId = this.accountService.getUserId()!;
    console.log("userId: " + this.managerId);

    this.route.paramMap.subscribe(params => {
      this.projectId = params.get('projectId')!;
      this.loadUsersAndProject();
    });
  }

  private loadUsersAndProject() {
    this.accountService.getAllUsers().subscribe({
      next: (users: UserResponse[]) => this.handleUsersResponse(users),
      error: (err) => this.handleUsersError(err)
    });
  }

  private handleUsersResponse(users: UserResponse[]) {
    this.allUsers = users.map(user => ({
      id: user.id,
      email: user.email
    }));

    this.loadProject();
  }

  private loadProject() {
    this.projectService.getProjectById(this.projectId).subscribe({
      next: (project: Project) => this.handleProjectResponse(project),
      error: (err) => this.handleProjectError(err)
    });
  }

  private handleProjectResponse(project: Project) {
    console.log('Project:', project);

    const userIds = project.user_ids || [];
    this.projectMembers = this.allUsers.filter(user => userIds.includes(user.id));
    this.project = project;

    this.minMembers = project.min_members;
    this.maxMembers = project.max_members;

    this.updateFilteredUsers();
  }

  private updateFilteredUsers() {
    this.filteredUsers = this.allUsers.filter(user =>
      !this.projectMembers.some(member => member.id === user.id)
    );
  }

  private handleUsersError(err: any) {
    console.error('Error retrieving users:', err);
  }

  private handleProjectError(err: any) {
    console.error('Error retrieving project:', err);
  }


  filterUsers() {
    this.filteredUsers = this.allUsers
      .filter(user => user.email.toLowerCase().includes(this.searchTerm.toLowerCase()))
      .filter(user => !this.projectMembers.some(member => member.id === user.id));
  }

  addMember(user: User) {
    if (!this.isFull() && !this.projectMembers.some(member => member.id === user.id)) {
      this.projectMembers.push(user);

      const memberId = user.id;

      this.projectService.addMembersToProject(this.projectId, [memberId]).subscribe({
        next: () => {
          console.log('Member added successfully:', memberId);
        },
        error: (err) => {
          console.error('Error adding member:', err);
        }
      });

      this.filterUsers();
    }
  }

  removeMember(userId: string) {

    if (this.projectMembers.length <= this.minMembers) {
      alert("Cannot remove member: Minimum number of members required.")
      return;
    }
    console.log(`Deleted user id-- ${userId}`);
    console.log(`Project id-- ${this.projectId}`);

    this.projectService.deleteMemberFromProject(this.projectId,userId).subscribe({
      next: () => {
        console.log(`User sa ID ${userId} je uspesoo obrisan.`);
        this.projectMembers = this.projectMembers.filter(member => member.id !== userId);
        this.filterUsers();
      },
      error: (error) =>{
        if(error.status == 409){
          alert("Cannot remove member: Member is added to active task.")
        }
        console.error('Greška prlikom .....', error)
      }
    });

  }


  isUserAssigned(user: User): boolean {
    return this.projectMembers.some(member => member.id === user.id);
  }

  isFull(): boolean {
    return this.projectMembers.length >= this.maxMembers;
  }


  addNewTask(projectId: string) {
    this.router.navigate([`/projects/${projectId}/addTask`]);

  }

  addNewProject() {
    this.router.navigate(['/projects']);
  }
}

