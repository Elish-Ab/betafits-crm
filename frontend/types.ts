
export enum SalesStage {
  Target = 'Target',
  Contact = 'Contact',
  Meeting = 'Meeting',
  Opportunity = 'Opportunity',
  Consulting = 'Consulting',
  FinishLine = 'Finish Line',
  Customer = 'Customer',
  ClosedLost = 'Closed Lost'
}

export enum PartnerStage {
  Identified = 'Identified',
  Outreach = 'Outreach',
  Meeting = 'Meeting',
  DueDiligence = 'Due Diligence',
  Agreement = 'Agreement',
  Active = 'Active',
  Lost = 'Lost'
}

export enum PartnerType {
  VC_PE = 'VC/PE',
  FractionalCFO = 'Fractional CFO',
  FractionalHR = 'Fractional HR',
  Other = 'Other'
}

export interface Company {
  id: string;
  name: string;
  industry?: string;
  employees?: number;
  usEmployees?: number;
  internationalEmployees?: number;
  city?: string;
  state?: string;
  peo?: string;
  website?: string;
  linkedin?: string;
  status: 'Active' | 'Inactive';
  icpScore: number; // 0-100
  // 5500 Data
  ein?: string;
  planAssets?: number;
  participantCount?: number;
  renewalMonth?: string;
}

export interface Prospect {
  id: string;
  companyId: string;
  companyName: string;
  stage: SalesStage;
  value: number;
  probability: number;
  owner: string;
  source: string;
  type: 'Sales'; 
  expectedCloseDate?: string;
  icpScore: number; // Denormalized for easy access in Kanban
}

export interface PartnerOpportunity {
  id: string;
  partnerId: string;
  partnerName: string;
  stage: PartnerStage;
  type: PartnerType;
  owner: string;
  notes?: string;
  probability: number;
}

export interface Partner {
  id: string;
  name: string;
  type: PartnerType;
  contactName?: string;
  email?: string;
  referralsMade: number;
  activePartnership: boolean;
  stage: PartnerStage;
  // Market Intel
  description?: string;
  foundedYear?: number;
  website?: string;
  linkedin?: string;
  investmentFocus?: string;
  location?: string;
}

export interface Interaction {
  id: string;
  date: string;
  type: 'Call' | 'Email' | 'Meeting' | 'LinkedIn';
  summary: string;
  companyId?: string;
  prospectId?: string;
  partnerId?: string;
  user: string;
}

export interface UniverseRecord {
  id: string;
  name: string;
  ein: string;
  industry: string;
  location: string;
  employees: number;
  planAssets: number;
  peo: string;
  renewalMonth: string;
  icpScore: number;
  isTarget: boolean;
  inPipeline: boolean;
}

export interface PartnerUniverseRecord {
  id: string;
  name: string;
  category: PartnerType;
  location: string;
  focus: string;
  icpScore: number;
  isTarget: boolean;
  inPipeline: boolean;
}

export interface DashboardMetrics {
  totalProspects: number;
  pipelineValue: number;
  winRate: number;
  activeReferrals: number;
}

// Analytics Types
export interface PEOStats {
  id: string;
  name: string;
  clientCount: number;
  wcPolicyCount?: number;
  avgClientSize?: number;
  trend?: 'up' | 'down' | 'flat';
}

export interface DocumentProviderStats {
  id: string;
  name: string;
  providerName: string;
  clientCount: number;
  marketShare?: number;
}

export interface Admin316Stats {
  id: string;
  name: string;
  ein: string;
  clientCount: number;
}
