import { Job, User } from '../types.js';

export class PoolPermission {
    public constructor(public user: User) {
    }
    public get userIsAdministrator(): boolean {
        // Implement your logic to check if the user is an administrator
        return true; // Example: assume all users are administrators
    }
    public canKillJob(job: Job): boolean {
        // Implement your permission logic here
        return this.userIsAdministrator || job.userId === this.user.id; // allow all jobs to be killed if user is admin or owns the job
    }
}