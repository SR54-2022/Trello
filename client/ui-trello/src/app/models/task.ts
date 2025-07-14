export type TaskStatus = 'Pending' | 'In Progress' | 'Completed';

export interface TaskParams {
  id: string;
  projectId: string;
  name: string;
  description: string;
  status?: TaskStatus;
  createdAt: Date;
  updatedAt: Date;
  user_ids?: string[];
  dependencies?: string[];
  blocked?: boolean;
}

export class Task {
  id: string;
  projectId: string;
  name: string;
  description: string;
  status: TaskStatus;
  createdAt: Date;
  updatedAt: Date;
  user_ids: string[];
  dependencies: string[];
  blocked: boolean;

  constructor({
                id,
                projectId,
                name,
                description,
                status = 'Pending',
                createdAt,
                updatedAt,
                user_ids = [],
                dependencies = [],
                blocked = false,
              }: TaskParams) {
    this.id = id;
    this.projectId = projectId;
    this.name = name;
    this.description = description;
    this.status = status;
    this.createdAt = createdAt;
    this.updatedAt = updatedAt;
    this.user_ids = user_ids;
    this.dependencies = dependencies;
    this.blocked = blocked;
  }
}
