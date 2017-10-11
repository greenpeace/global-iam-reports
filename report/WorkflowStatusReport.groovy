package reports

import org.apache.commons.collections.CollectionUtils
import org.openiam.base.OrderConstants
import org.openiam.base.ws.SortParam
import org.openiam.idm.searchbeans.AuditLogSearchBean
import org.openiam.idm.searchbeans.ResourceSearchBean
import org.openiam.idm.searchbeans.ResourceTypeSearchBean
import org.openiam.idm.searchbeans.RoleSearchBean
import org.openiam.idm.srvc.audit.domain.WorkflowStatusRptViewEntity
import org.openiam.idm.srvc.audit.dto.IdmAuditLog
import org.openiam.idm.srvc.audit.service.AuditLogService
import org.openiam.idm.srvc.grp.service.GroupDataService
import org.openiam.idm.srvc.lang.dto.Language
import org.openiam.idm.srvc.mngsys.domain.AssociationType
import org.openiam.idm.srvc.org.service.OrganizationDataService
import org.openiam.idm.srvc.report.dto.ReportDataDto
import org.openiam.idm.srvc.report.dto.ReportQueryDto
import org.openiam.idm.srvc.report.dto.ReportRow
import org.openiam.idm.srvc.report.dto.ReportRow.ReportColumn
import org.openiam.idm.srvc.report.dto.ReportTable
import org.openiam.idm.srvc.report.service.ReportDataSetBuilder
import org.openiam.idm.srvc.res.dto.Resource
import org.openiam.idm.srvc.res.dto.ResourceType
import org.openiam.idm.srvc.res.service.ResourceDataService
import org.openiam.idm.srvc.role.domain.RoleEntity
import org.openiam.idm.srvc.role.service.RoleDataService
import org.openiam.idm.srvc.user.dto.User
import org.openiam.idm.srvc.user.ws.UserDataWebService
import org.springframework.beans.BeansException
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.openiam.idm.srvc.mngsys.ws.ManagedSystemWebService
import org.openiam.idm.srvc.mngsys.dto.ApproverAssociation
import org.openiam.idm.srvc.mngsys.dto.ApproverAssocationSearchBean

import org.openiam.idm.srvc.audit.service.WorkflowStatusRptViewDAO

import java.text.DateFormat
import java.text.ParseException

public class WorkflowStatus implements ReportDataSetBuilder {

    final static String DEFAULT_REQUESTER_ID = "3000"
    final static Language DEFAULT_LANGUAGE = new Language(id:  1)
    final static String NOT_FOUND = '[ Not found ]'
    final static String DELETED = '[ DELETED ]'

    def masterTableKeys = new HashSet<String>()

    private ApplicationContext context
    private UserDataWebService userWebService
    private AuditLogService auditLogService
    private ResourceDataService resourceService
    private RoleDataService roleService
    private GroupDataService groupService
    private OrganizationDataService organizationService
    private ManagedSystemWebService managedSystemService

    @Autowired
    private WorkflowStatusRptViewDAO workDAO

    DateFormat dateFormat

    @Override
    public ReportDataDto getReportData(ReportQueryDto query) {

        println "WorkflowStatus data source, request: " + query.queryParams.values().join(", ")
        userWebService = context.getBean("userWS") as UserDataWebService
        auditLogService = context.getBean(AuditLogService.class)
        resourceService = context.getBean(ResourceDataService.class)
        roleService = context.getBean(RoleDataService.class)
        groupService = context.getBean(GroupDataService.class)
        organizationService = context.getBean(OrganizationDataService.class)
        managedSystemService = context.getBean("managedSysService") as ManagedSystemWebService

        workDAO = context.getBean("workflowStatusRptViewDAOImpl") as WorkflowStatusRptViewDAO

        def String ownerId = query.getParameterValue("OWNER_ID")
        //def String resTypeId = query.getParameterValue("RES_TYPE_ID")
        def List<String> resIds = query.getParameterValues("RES_IDS")
        def String periodStartString = query.getParameterValue("PERIOD_START")
        def String periodEndString = query.getParameterValue("PERIOD_END")
        def Date periodStart = dateFromString(periodStartString)
        def Date periodEnd = dateFromString(periodEndString)
        def String eventId = query.getParameterValue("ACTION_ID")

        def ReportTable reportTable = new ReportTable()

        def isHeadRequest = query.getParameterValue("TABLE") == "HEAD"

        if (isHeadRequest) {

            def ReportRow row = new ReportRow()
            reportTable.name = "head"
            if (ownerId) {
                row.column.add(new ReportColumn('OWNER', getUserFullName(ownerId)))
            }
            if (!EmptyMultiValue(resIds)) {

                def targetName = ""
                resIds.each { resId ->
                    def resName = findResource(resId)?.name
                    if (resName) {
                        targetName += (targetName ? ", " : "") + resName
                    }
                }
                row.column.add(new ReportColumn('RESOURCE', targetName ?: NOT_FOUND))
            }
            if (eventId) {
                row.column.add(new ReportColumn('ACTION_ID', eventId))
            }

            if (periodStart) {
                def period = "from " + dateToString(periodStart)
                row.column.add(new ReportColumn('PERIOD_START', periodStart.time.toString()))
                if (periodEnd) {
                    period += " to " + dateToString(periodEnd)
                    row.column.add(new ReportColumn('PERIOD_END', periodEnd.time.toString()))
                }
                row.column.add(new ReportColumn('PERIOD', period))
            }
            reportTable.row.add(row)

        } else {

            reportTable.name = "details"
            def messages = validateParameters(ownerId, resIds, periodStart, periodEnd) as String[]
            if (messages) {
                for (def msg : messages) {
                    def ReportRow row = new ReportRow()
                    row.column.add(new ReportColumn('ERROR', msg))
                    reportTable.row.add(row)
                }
            } else {

                if(resIds != null) {
                    if ('addUserToRole'.equals(eventId) || 'removeUserFromRole'.equals(eventId)) {
                        List<String> roleIds = getRoleIdForResource(resIds);
                        for (def i = 0; i < roleIds.size(); i++) {
                            def List<WorkflowStatusRptViewEntity> workflowStatusRptViewEntityList = workDAO.getResultForReport(periodStart, periodEnd, eventId, roleIds.get(i))
                            for (WorkflowStatusRptViewEntity w : workflowStatusRptViewEntityList){
                                addMainTableRow(w, reportTable);
                            }
                        }
                    } else {
                        for (def i = 0; i < resIds.size(); i++) {
                            def List<WorkflowStatusRptViewEntity> workflowStatusRptViewEntityList = workDAO.getResultForReport(periodStart, periodEnd, eventId, resIds.get(i))
                            for (WorkflowStatusRptViewEntity workflowStatus : workflowStatusRptViewEntityList) {
                                addMainTableRow(workflowStatus, reportTable);
                            }
                        }
                    }
                } else {
                    def List<WorkflowStatusRptViewEntity> workflowStatusRptList = workDAO.getResultForReport(periodStart, periodEnd, eventId, null)
                    for (WorkflowStatusRptViewEntity workflowStatus : workflowStatusRptList) {
                        addMainTableRow(workflowStatus, reportTable);
                    }
                }
            }
        }

        return new ReportDataDto( tables : [ reportTable ] as List<ReportTable> )
    }

    private void addMainTableRow(WorkflowStatusRptViewEntity workflowStatus, ReportTable reportTable) {

                    ReportRow row = new ReportRow()
                    row.column.add(new ReportColumn('EMPLOYEE', workflowStatus.employeeFirstName + " " + workflowStatus.employeeLastName))
                    row.column.add(new ReportColumn('OWNER', workflowStatus.approverFirstName + " " + workflowStatus.approverLastName))
                    row.column.add(new ReportColumn('RESOURCE', workflowStatus.associationName))
                    row.column.add(new ReportColumn('LAST_DATE', dateToString(workflowStatus.approvalDate)))
                    row.column.add(new ReportColumn('APPROVED', workflowStatus.isApproved))
                    row.column.add(new ReportColumn('APPROVER', workflowStatus.requesterFirstName + " " + workflowStatus.requesterLastName))

                    reportTable.row.add(row)
    }

    static def validateParameters(String ownerId, List<String> resIds, Date periodStart, Date periodEnd) {
        def violations = [] as List
        if (!periodStart)
            violations.add "Parameter 'Period start' is required"
        if (periodStart && periodEnd && periodEnd < periodStart)
            violations.add "Parameter 'Period start' has to be less than 'Period end'"
        return violations
    }

    @Override
    void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext
    }

    def String getUserFullName(String userId) {
        def User user = userWebService.getUserWithDependent(userId, DEFAULT_REQUESTER_ID, false)
        return user ? (user.firstName +
                (user.middleInit ? ' ' + user.middleInit : '') +
                (user.lastName ? ' ' + user.lastName : '')) : DELETED;
    }

    def KeyNameBean findResource(String resId) {
        // search resource
        def resourceSearchBean = new ResourceSearchBean( key: resId )
        def resources = resourceService.findBeans(resourceSearchBean, 0, 1, DEFAULT_LANGUAGE) as List<Resource>
        if (resources) {
            def res = resources.get(0)
            def displayName = "[" + res.resourceType?.displayName + "] " +
                    (res.displayName ?: res.coorelatedName ?: res.name)

            return new KeyNameBean(id: res.id, name: displayName)
        }
        return null
    }

    def KeyNameBean findResource(Collection<String> resIds, String resTypeId) {
        for(String resId : resIds) {
            def resourceSearchBean = new ResourceSearchBean(key: resId, resourceTypeId: resTypeId)
            def resources = resourceService.findBeans(resourceSearchBean, 0, 1, DEFAULT_LANGUAGE) as List<Resource>
            if (resources) {
                def res = resources.get(0)
                return new KeyNameBean(id: res.id, name: res.displayName)
            }
        }
        return null
    }

    def KeyNameBean findResourceType(String resTypeId) {
        def searchBean = new ResourceTypeSearchBean( key: resTypeId )
        def types = resourceService.findResourceTypes(searchBean, 0, 1, DEFAULT_LANGUAGE) as List<ResourceType>
        if (types) {
            def type = types.get(0)
            return new KeyNameBean(id: type.id, name: type.displayName ?: type.id)
        }
        return null
    }

    private class KeyNameBean { def String id; def String name; }

    private static boolean EmptyMultiValue(List<String> values) {
        return !values || (values.size() == 1 && !values[0])
    }

    Date dateFromString(String periodStartString) {
        try {
            return dateFormat && periodStartString ? dateFormat.parse(periodStartString) : null
        } catch (ParseException ignored) {
            return null
        }
    }

    String dateToString(Date date) {
        return (date && dateFormat) ? dateFormat.format(date) : ""
    }

    private List<String> getRoleIdForResource(List<String> resIds){
        if (resIds != null) {
            List<String> roleIds = new ArrayList<String>();
            for (def i = 0; i < resIds.size(); i++) {
                List<RoleEntity> roleEntityList = roleService.getRolesForResource(resIds.get(i), DEFAULT_REQUESTER_ID, 0, Integer.MAX_VALUE);

                for (RoleEntity roleEntity : roleEntityList) {
                    roleIds.add(roleEntity.id);
                }
            }
            if (roleIds != null) {
                return roleIds;
            }
            return null;
        } else
            return null;
    }
}
