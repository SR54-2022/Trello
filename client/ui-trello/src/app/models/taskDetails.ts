import { UserDetails } from "./userDetails";

export type TaskStatus = 'Pending' | 'In Progress' | 'Completed';

export interface TaskDetailsParams {
  id: string;
  projectId: string;
  name: string;
  description: string;
  status: TaskStatus;
  createdAt: Date;
  updatedAt: Date;
  userIds: string[];
  user_ids: string[];
  users: UserDetails[];
  dependencies: string[];
  blocked: boolean;
}

export class TaskDetails {
  id: string;
  projectId: string;
  name: string;
  description: string;
  status: TaskStatus;
  createdAt: Date;
  updatedAt: Date;
  userIds: string[];
  user_ids: string[];
  users: UserDetails[];
  dependencies: string[];
  blocked: boolean;

  constructor(params: TaskDetailsParams) {
    this.id = params.id;
    this.projectId = params.projectId;
    this.name = params.name;
    this.description = params.description;
    this.status = params.status;
    this.createdAt = params.createdAt;
    this.updatedAt = params.updatedAt;
    this.userIds = params.userIds;
    this.user_ids = params.user_ids;
    this.users = params.users;
    this.dependencies = params.dependencies;
    this.blocked = params.blocked;
  }
}
