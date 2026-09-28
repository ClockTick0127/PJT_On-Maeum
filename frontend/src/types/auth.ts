export type AuthTokens = {
  accessToken: string;
  refreshToken: string;
  expiresIn: number;
};

export type UserProfile = {
  userId: string;
  email: string;
  displayName: string;
  phone: string | null;
  organization: string | null;
  status: string;
  passwordResetRequired: boolean;
};

export type MeResponse = {
  profile: UserProfile;
  households: unknown[];
};
