export interface BeopenUser {
  id: string;
  language: string;
  gender: string;
  username: string;
  email: string;
  modifiedBy: string;
  createdAt: Date;
  modifiedAt: Date;
  age?: number;
  pilot?: string;
}
