import {Component, OnInit} from '@angular/core';
import {FormBuilder, FormGroup, Validators, ReactiveFormsModule} from "@angular/forms";
import {ToastrService} from "ngx-toastr";
import {Project} from "../models/project.model";
import {ProjectServiceService} from "../services/project-service.service";
import {HttpClient} from "@angular/common/http";
import {animate, state, style, transition, trigger} from "@angular/animations";
import {CommonModule} from '@angular/common';
import {Router} from "@angular/router";
import {dateValidator} from "../validator/date-validator";

import {MenuComponent} from "../menu/menu.component";
import {AccountService} from "../services/account.service";
import {DeleteService} from "../services/delete.service";
import {FormToggleService} from "../form-toggle.service";


@Component({
  selector: 'app-add-project',
  standalone: true,
  imports: [ReactiveFormsModule, CommonModule, MenuComponent],
  templateUrl: './add-project.component.html',
  styleUrls: ['./add-project.component.scss'],
  animations: [
    trigger('flyInOut', [
      state('in', style({ opacity: 1 })),
      transition('void => *', [
        style({ opacity: 0 }),
        animate(300)
      ]),
      transition('* => void', [
        animate(300, style({ opacity: 0 }))
      ])
    ])
  ]
})
export class AddProjectComponent implements OnInit {

  newProjectForm!: FormGroup;
  projects: any;
  manager: any;
  showForm: boolean = false;
  visible: boolean = false;
  selectedProjectId: string | null = null;

  toggleForm() {
    this.showForm = !this.showForm;
  }
  showDeleteConfirmation(id: string): void {
    this.visible = !this.visible;
    this.selectedProjectId = id;
  }
  cancel(){
    this.visible = !this.visible;
  }

  constructor(
    private readonly formBuilder: FormBuilder,
    private readonly projectService: ProjectServiceService,
    private readonly toaster: ToastrService,
    private readonly http: HttpClient,
    private readonly router: Router,
    private readonly accService: AccountService,
    private readonly deleteService: DeleteService,
    private readonly toastr: ToastrService,
    private readonly formToggleService: FormToggleService,
  ) { }

  ngOnInit(): void {
    this.fetchManager();
    this.fetchData();
    this.formToggleService.showForm$.subscribe((state) => {
      this.showForm = state;
    });
    this.newProjectForm = this.formBuilder.group({
      project_name: ['', [Validators.required, Validators.minLength(3)]],
      end_date: ['', [Validators.required, dateValidator()]],
      min_members: ['', [Validators.required]],
      max_members: ['', [Validators.required]],
    });
  }

  onSubmit() {
    if (this.newProjectForm.valid) {
      const newProjectRequest: Project = {
        name: this.newProjectForm.get('project_name')?.value,
        end_date: new Date(this.newProjectForm.get('end_date')?.value),
        min_members: this.newProjectForm.get('min_members')?.value.toString(),
        max_members: this.newProjectForm.get('max_members')?.value.toString(),
        manager: this.manager.id,
        user_ids: [],
      };
      console.log(newProjectRequest);
      this.projectService.addProject(newProjectRequest).subscribe({
        next: (result) => {
          this.toaster.success("Project added successfully!");
          this.newProjectForm.reset();
          this.fetchData();
        },
        error: (error) => {
          console.error('Error:', error);
          this.toaster.error('Adding project failed');
        }
      });
    } else {
      console.log(this.newProjectForm);
      this.newProjectForm.markAllAsTouched();
    }
  }

  fetchData() {
    this.http.get<Project[]>('/api/project-server/projects').subscribe({
      next: (response) => {
        this.projects = response.slice().reverse();
        console.log('Projects fetched successfully:', this.projects);
      },
      error: (error) => {
        console.error('Error fetching data:', error);

        if (error.status === 401) {
          this.router.navigate(['login']);
        } else {
          this.projects = [];
        }
      }
    });
  }

  fetchManager() {
    this.http.get('/api/user-server/manager')
      .subscribe({
        next: (response) => {
          this.manager = response;
          console.log('Manager:', this.manager);
          this.fetchData();
        },
        error: (error) => {
          console.error('Error fetching manager data:', error);
        }
      });
  }

  manageMembersToProject(id: string) {
    this.router.navigate(['/project/', id]);
  }

  deleteProject(){
    console.log(`Deleting project with ID: ` + this.selectedProjectId);
    if(this.selectedProjectId != null){
      this.deleteService.deleteProject(this.selectedProjectId).subscribe({
        next: () => {
          this.toastr.success("Succesfully deleted project.");
          console.log("Succesfully deleted project: " + this.selectedProjectId);
          this.visible = false;
          this.fetchData();

        },
        error: (error) => {
          this.toastr.error("Error deleted project: " + error.error);
          console.log("Error deleting project: ", error.error);

        }


      });

    }

  }



}
