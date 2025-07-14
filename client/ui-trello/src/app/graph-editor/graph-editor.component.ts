// graph-editor.component.ts

import { Component, ElementRef, Input, OnInit } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { GraphService } from "../services/graph.service";
import { Edge, TaskNode } from "../models/task-graph";
import { Network } from "vis-network";

@Component({
  selector: 'app-graph-editor',
  template: `<div id="graph" style="width: 100%; height: 600px; border: 2px solid #ddd; position: relative;"></div>
  <div id="tooltip" style="
    position: absolute;
    padding: 8px;
    background-color: rgba(0, 0, 0, 0.8);
    color: white;
    border-radius: 4px;
    display: none;
    pointer-events: none;
    font-size: 12px;">
  </div>
  `,
  styles: [],
})
export class GraphEditorComponent implements OnInit {
  nodes: TaskNode[] = [];
  links: Edge[] = [];
  @Input() projectId!: string;

  constructor(private http: HttpClient, private elRef: ElementRef, private graphService: GraphService) {}

  ngOnInit() {
    this.fetchData();
  }

  fetchData() {
    this.graphService.getWorkflowByProject(this.projectId).subscribe((data) => {
      this.nodes = data.nodes;
      this.links = data.edges;
      this.createGraph();
    });
  }

  createGraph() {
    const visNodes = this.nodes.map((node) => ({
      id: node.id,
      label: node.label,
      description: node.description,
      blocked: node.blocked,
      isComplete: node.isComplete,
      color: {
        background: node.isComplete ? '#2db629' : node.blocked ? '#FF7F7F' : '#F2BB05',
        border: '#4E4E55',
        highlight: {
          background: node.isComplete ? '#2db629' : node.blocked ? '#FF5A5F' : '#F2BB05',
          border: '#4E4E55',
        },
        hover: {
          background: node.isComplete ? '#7FFF7F' : node.blocked ? '#FF7F7F' : '#FBC823',
          border: '#4E4E55',
        },
      },
      status: node.status,
    }));

    const visEdges = this.links.map((link) => ({
      from: link.from,
      to: link.to,
    }));

    const data = {
      nodes: visNodes,
      edges: visEdges,
    };


    const options = {
      nodes: {
        shape: 'dot',
        size: 20,
        font: { size: 14 },
      },
      edges: {
        width: 2,
        color: { color: '#4E4E55', hover: '#4E4E55' },
        arrows: { to: { enabled: true } },
      },
      physics: {
        enabled: true,
        solver: 'forceAtlas2Based',
        forceAtlas2Based: {
          gravitationalConstant: -26,
          centralGravity: 0.01,
          springLength: 100,
          springConstant: 0.08,
        },
        minVelocity: 0.75,
      },
      interaction: {
        dragNodes: true,
        dragView: true,
        zoomView: true,
        hover: true,
      },
    };

    const container = this.elRef.nativeElement.querySelector('#graph');
    const tooltip = this.elRef.nativeElement.querySelector('#tooltip');
    const network = new Network(container, data, options);


    network.on('hoverNode', (params) => {
      const nodeId = params.node;
      const node = visNodes.find((n) => n.id === nodeId);

      if (node) {
        tooltip.style.display = 'block';

        const description = node.description ?? 'No description available';
        const status = node.status ?? 'No status available';

        tooltip.innerText = `Description: ${description}\nStatus: ${status}`;
      }
    });


    network.on('blurNode', () => {
      tooltip.style.display = 'none';
    });

    container.addEventListener('mousemove', (event: MouseEvent) => {
      tooltip.style.left = `${event.pageX + 10}px`;
      tooltip.style.top = `${event.pageY + 10}px`;
    });
  }



}
