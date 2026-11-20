import { Job, User } from '../types.js';

export class PoolPermission {
    constructor(public user: User) {
    }
    get userIsAdministrator(): boolean {
        // Implement your logic to check if the user is an administrator
        return true; // Example: assume all users are administrators
    }
    canKillJob(job: Job): boolean {
        // Implement your permission logic here
        return this.userIsAdministrator || job.userId === this.user.id; // allow all jobs to be killed if user is admin or owns the job
    }
    get canControlDispatcher(): boolean {
        return this.userIsAdministrator; // only administrators can control the dispatcher
    }
}