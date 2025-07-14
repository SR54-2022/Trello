import { Component, OnInit } from '@angular/core';
import { AnalyticsService } from "../services/analytics.service";
import { ActivatedRoute } from "@angular/router";
import { ProjectDetails } from "../models/projectDetails";
import { ProjectServiceService } from "../services/project-service.service";
import { TaskDetails } from "../models/taskDetails";
import { UserDetails } from "../models/userDetails";

interface TaskTimeSpent {
  task_id: string;
  time_in_state: {
    state: string;
    start: string;
    end?: string;
  }[];
}

interface UserOnProject {
  user_id: string;
  tasks_assigned: { task_id: string }[];
}

interface Analytics {
  _id: string;
  completion_estimate: string;
  last_updated: string;
  project_id: string;
  project_on_schedule: boolean | string;
  tasks_by_status: {
    Completed: number;
    'In Progress': number;
    Pending: number;
  };
  tasks_time_spent: TaskTimeSpent[];
  total_tasks: number;
  users_on_project: UserOnProject[];
}

@Component({
  selector: 'app-analytics',
  templateUrl: './analytics.component.html',
  styleUrls: ['./analytics.component.css']
})
export class AnalyticsComponent implements OnInit {
  projectID: string = '';
  analytics: Analytics | null = null;
  project: ProjectDetails | null = null;
  tasks: TaskDetails[] = [];
  private allUsers!: UserDetails[];
  taskMap: Map<string, string> = new Map(); // Map task ID to task name
  userMap: Map<string, string> = new Map(); // Map user ID to user name

  constructor(
    private eventService: AnalyticsService,
    private route: ActivatedRoute,
    private projectService: ProjectServiceService
  ) {}

  ngOnInit(): void {
    this.projectID = this.route.snapshot.paramMap.get('projectId') ?? '';
    if (this.projectID) {
      this.loadAnalytics();
      this.loadProjectDetails(this.projectID);
    }
  }

  loadAnalytics(): void {

    this.eventService.getAnalytics(this.projectID).subscribe({
      next: (data: Analytics) => {
        this.analytics = data;
        this.analytics.project_on_schedule =
          this.analytics.project_on_schedule == null
            ? 'Project is not finished yet.'
            : (this.analytics.project_on_schedule ? 'The project is finished on schedule.' : 'The project is not finished on time, the estimated completion date is exceeded.');
      },
      error: error => {
        console.error('Error fetching analytics:', error);
      }
    })

  }

  loadProjectDetails(projectId: string): void {
    this.projectService.getProjectDetailsById(projectId).subscribe({
      next: (data: ProjectDetails) => {
        this.project = data;
        this.tasks = this.project.tasks;

        if (this.tasks && this.tasks.length > 0) {
          this.tasks.forEach(task => {
            this.taskMap.set(task.id, task.name);
          });
        }
        const userIds = this.project.user_ids;
        if (!Array.isArray(userIds) || userIds.length === 0) {
          console.warn('No userIds provided, skipping fetch.');
          return;
        }
        console.log('user ids are ' + userIds);

        // Fetch user details
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

            // Create a map of user ID to user name
            this.allUsers.forEach(user => {
              this.userMap.set(user.id, user.email); // Assuming `user.name` holds the user's name
            });
            console.log('usermap size is ' + this.userMap.size);
          })
          .catch(error => {
            console.error('Error fetching user details:', error);
          });
      },
      error: (err) => {
        console.error('Error loading project details:', err);
      }
    });
  }


  getDuration(start: string, end?: string): string {
    if (!start) return '0 minutes';

    const startTime = new Date(start).getTime();
    const endTime = end ? new Date(end).getTime() : Date.now();

    const durationMs = endTime - startTime;
    const hours = Math.floor(durationMs / (1000 * 60 * 60));
    const minutes = Math.floor((durationMs % (1000 * 60 * 60)) / (1000 * 60));

    if (hours > 0) {
      return `${hours} hours ${minutes} minutes`;
    } else {
      return `${minutes} minutes`;
    }
  }

}
