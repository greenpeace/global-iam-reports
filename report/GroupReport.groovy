package reports

import org.openiam.base.ws.SortParam
import org.openiam.idm.searchbeans.GroupSearchBean
import org.openiam.idm.searchbeans.UserSearchBean
import org.openiam.idm.srvc.grp.domain.GroupAttributeEntity
import org.openiam.idm.srvc.grp.domain.GroupEntity
import org.openiam.idm.srvc.grp.service.GroupDAO
import org.openiam.idm.srvc.grp.service.GroupDataService
import org.openiam.idm.srvc.lang.dto.Language
import org.openiam.idm.srvc.mngsys.service.ManagedSystemService
import org.openiam.idm.srvc.org.dto.Organization
import org.openiam.idm.srvc.org.service.OrganizationDataService
import org.openiam.idm.srvc.report.dto.ReportDataDto
import org.openiam.idm.srvc.report.dto.ReportQueryDto
import org.openiam.idm.srvc.report.dto.ReportRow
import org.openiam.idm.srvc.report.dto.ReportRow.ReportColumn
import org.openiam.idm.srvc.report.dto.ReportTable
import org.openiam.idm.srvc.report.service.ReportDataSetBuilder
import org.openiam.idm.srvc.user.domain.UserEntity
import org.openiam.idm.srvc.user.service.UserDataService
import org.springframework.beans.BeansException
import org.springframework.context.ApplicationContext
import java.text.DateFormat

public class GroupReport implements ReportDataSetBuilder {

    final static String NOT_FOUND = '[ Not found ]'
    final static String DEFAULT_REQUESTER_ID = "3000"
    final static Language DEFAULT_LANGUAGE = new Language(id: 1)
    final static int GROUP_PAGE_SIZE = 5000

    private ApplicationContext context
    DateFormat dateFormat

    private Long startTime = 0
    final static int PROCESSING_TIMEOUT = 290

    @Lazy UserDataService userDataService = {context.getBean("userManager")}()
    @Lazy OrganizationDataService organizationService = {context.getBean("orgManager")}()
    @Lazy ManagedSystemService managedSystemService = {context.getBean(ManagedSystemService.class)}()
    @Lazy GroupDataService groupService = {context.getBean(GroupDataService.class)}()
    @Lazy GroupDAO groupDAO = {context.getBean(GroupDAO.class)}()

    @Override
    void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext
    }

    @Override
    public ReportDataDto getReportData(ReportQueryDto query) {

        println ">>> GroupReport data source, request: " + query.queryParams.values().join(", ")

        startTime = new Date().time

        String[] groupIds = query.getParameterValues("GROUP_ID")
        String orgId = query.getParameterValue("ORG_ID")
        String risk = query.getParameterValue("RISK")
        String manSysId = query.getParameterValue("MANAGED_SYS_ID")
        String userId = query.getParameterValue("USER_ID")
        boolean details = "Y" == query.getParameterValue("DETAILS")
        boolean members = "Y" == query.getParameterValue("MEMBERS")
        String outputTable = query.getParameterValue("TABLE")

        ReportTable reportTable = new ReportTable()

        if (outputTable == "HEAD") {

            reportTable.name = "head"
            ReportRow row = new ReportRow()
            if (!EmptyMultiValue(groupIds)) {
                def ids = new HashSet<String>(Arrays.asList(groupIds))
                def searchBean = new GroupSearchBean(keys: ids)
                def groups = groupService.findBeans(searchBean, DEFAULT_REQUESTER_ID, 0, GROUP_PAGE_SIZE)?.collect { it.name }
                row.column.add(new ReportColumn('GROUPS', groups ? groups.join(', ') : NOT_FOUND))
            }
            if (orgId) {
                def Organization bean = organizationService.getOrganizationLocalized(orgId, null, DEFAULT_LANGUAGE)
                row.column.add(new ReportColumn('ORGANIZATION', bean?.name ?: NOT_FOUND))
            }
            if (risk) {
                row.column.add(new ReportColumn('RISK', risk?.toUpperCase()))
            }
            if (manSysId) {
                def manSystem = managedSystemService.getManagedSysById(manSysId)
                row.column.add(new ReportColumn('MANAGED_SYSTEM', manSystem?.name ?: NOT_FOUND))
            }
            if (userId) {
                def searchBean = new UserSearchBean(deepCopy: false, key: userId)
                def users = userDataService.findBeans(searchBean)
                if (users) {
                    def user = users.get(0)
                    def fullName = user.firstName + ' ' + (user.middleInit ? user.middleInit + ' ' : '') + user.lastName
                    row.column.add(new ReportColumn('USER', user ? fullName : NOT_FOUND))
                }
            }
            if (details) {
                row.column.add(new ReportColumn('DETAILS', 'Y'))
            }
            if (members) {
                row.column.add(new ReportColumn('MEMBERS', 'Y'))
            }
            reportTable.row.add(row)

        } else {

            reportTable.setName("details")
            GroupSearchBean searchBean = new GroupSearchBean()

            if (groupIds) {
                def ids = new HashSet<String>(Arrays.asList(groupIds))
                searchBean.keys = ids
            }
            if (manSysId) {
                searchBean.managedSysId = manSysId
            }
            if (orgId) {
                searchBean.addOrganizationId(orgId)
            }
            if (userId) {
                searchBean.addUserId(userId)
            }
            if (risk) {
                searchBean.risk = getRiskId(risk)
            }
            searchBean.sortBy = [new SortParam("name"), new SortParam("id")] as List
            int counter = 0
            int elapsed = 0

            while (PROCESSING_TIMEOUT > elapsed) {

                List<GroupEntity> groups = groupService.findBeans(searchBean, DEFAULT_REQUESTER_ID, counter, GROUP_PAGE_SIZE)
                if (!groups) {
                    break
                }

                for (GroupEntity g : groups) {
                    def ReportRow row = new ReportRow()
                    row.column.add(new ReportColumn('DETAILS_TYPE', '0'))
                    row.column.add(new ReportColumn('GROUP_ID', g.id))
                    row.column.add(new ReportColumn('GROUP_NAME', g.name ?: '-'))
                    row.column.add(new ReportColumn('MANAGED_SYSTEM', g.managedSystem?.name ?: '-'))
                    row.column.add(new ReportColumn('CLASSIFICATION', g.classification?.description ?: '-'))
                    row.column.add(new ReportColumn('RISK', g.risk?.description ?: '-'))
                    row.column.add(new ReportColumn('ORGANIZATION', g.organizationSet?.collect { it.name }?.join(', ') ?: '-'))
                    row.column.add(new ReportColumn('DESCRIPTION', g.description ?: '-'))
                    row.column.add(new ReportColumn('MEMBERS_COUNT',
                            userDataService.getNumOfUsersForGroup(g.id, DEFAULT_REQUESTER_ID).toString()))
                    reportTable.row.add(row)
                    if (details) {
                        g.attributes.sort { it.name }.each { GroupAttributeEntity attr ->
                            row = new ReportRow()
                            row.column.add(new ReportColumn('DETAILS_TYPE', '1'))
                            row.column.add(new ReportColumn('GROUP_ID', g.id))
                            row.column.add(new ReportColumn('GROUP_NAME', g.name ?: ''))
                            row.column.add(new ReportColumn('DETAILS_GROUP', 'Attributes' ?: ''))
                            row.column.add(new ReportColumn('DETAILS_NAME', attr.name ?: ''))
                            if (!attr.isMultivalued) {
                                row.column.add(new ReportColumn('DETAILS_VALUE', attr.value ?: ''))
                            } else {
                                row.column.add(new ReportColumn('DETAILS_VALUE', attr.values.join(', ') ?: ''))
                            }
                            reportTable.row.add(row)
                        }
                        g.parentGroups.sort { it.name }.each {
                            row = new ReportRow()
                            row.column.add(new ReportColumn('DETAILS_TYPE', '2'))
                            row.column.add(new ReportColumn('GROUP_ID', g.id))
                            row.column.add(new ReportColumn('GROUP_NAME', g.name ?: ''))
                            row.column.add(new ReportColumn('DETAILS_GROUP', 'Entitlements' ?: ''))
                            row.column.add(new ReportColumn('DETAILS_NAME', 'Parent group' ?: ''))
                            row.column.add(new ReportColumn('DETAILS_VALUE', it.name ?: ''))
                            reportTable.row.add(row)
                        }
                        g.childGroups.sort { it.name }.each {
                            row = new ReportRow()
                            row.column.add(new ReportColumn('DETAILS_TYPE', '2'))
                            row.column.add(new ReportColumn('GROUP_ID', g.id))
                            row.column.add(new ReportColumn('GROUP_NAME', g.name ?: ''))
                            row.column.add(new ReportColumn('DETAILS_GROUP', 'Entitlements' ?: ''))
                            row.column.add(new ReportColumn('DETAILS_NAME', 'Child group' ?: ''))
                            row.column.add(new ReportColumn('DETAILS_VALUE', it.name ?: ''))
                            reportTable.row.add(row)
                        }
                        g.roles.sort { it.name }.each {
                            row = new ReportRow()
                            row.column.add(new ReportColumn('DETAILS_TYPE', '2'))
                            row.column.add(new ReportColumn('GROUP_ID', g.id))
                            row.column.add(new ReportColumn('GROUP_NAME', g.name ?: ''))
                            row.column.add(new ReportColumn('DETAILS_GROUP', 'Entitlements' ?: ''))
                            row.column.add(new ReportColumn('DETAILS_NAME', 'Role' ?: ''))
                            row.column.add(new ReportColumn('DETAILS_VALUE', it.name ?: ''))
                            reportTable.row.add(row)
                        }
                        g.resources.sort { it.name }.each {
                            row = new ReportRow()
                            row.column.add(new ReportColumn('DETAILS_TYPE', '2'))
                            row.column.add(new ReportColumn('GROUP_ID', g.id))
                            row.column.add(new ReportColumn('GROUP_NAME', g.name ?: ''))
                            row.column.add(new ReportColumn('DETAILS_GROUP', 'Entitlements' ?: ''))
                            row.column.add(new ReportColumn('DETAILS_NAME', 'Resource' ?: ''))
                            row.column.add(new ReportColumn('DETAILS_VALUE', it.name ?: ''))
                            reportTable.row.add(row)
                        }
                    }
                    if (members) {
                        g.users.sort { it.firstName + it.lastName }.each { UserEntity user ->
                            row = new ReportRow()
                            row.column.add(new ReportColumn('DETAILS_TYPE', '3'))
                            row.column.add(new ReportColumn('GROUP_ID', g.id))
                            row.column.add(new ReportColumn('GROUP_NAME', g.name ?: ''))
                            row.column.add(new ReportColumn('DETAILS_GROUP', 'Member users' ?: ''))
                            def userName = user.firstName + ' ' + (user.middleInit ? user.middleInit + ' ' : '') + user.lastName
                            row.column.add(new ReportColumn('DETAILS_VALUE', userName ?: ''))
                            reportTable.row.add(row)
                        }
                    }
                    ++counter
                    if (counter % 200 == 0) {
                        println ">>> GroupReport: $counter groups processed. Elapsed time: $elapsed sec"
                        elapsed = ((new Date().time - startTime) / 1000) as int
                        if (PROCESSING_TIMEOUT <= elapsed) {
                            break
                        }
                    }
                }

                if (counter % (4*GROUP_PAGE_SIZE)) {
                    groupDAO.clear()
                }
            }

            if (PROCESSING_TIMEOUT <= elapsed) {
                println ">>> GroupReport: Processing timeout reached: $PROCESSING_TIMEOUT sec. $counter groups processed."
                // add warning
                def row = new ReportRow()
                def msg = "Processing timeout reached ($PROCESSING_TIMEOUT sec). " +
                        "Groups processed: $counter. " +
                        "Define stricter parameters to get complete report" as String
                row.column.add(new ReportColumn('WARNING', msg))
                reportTable.row.add(row)
            }

        }

        println ">>> GroupReport data source, response contains " + reportTable.row.size() + " rows"
        return packReportTable(reportTable)
    }

    private static String getRiskId(String value) {
        switch (value?.toUpperCase()) {
            case "HIGH": return "HIGH_RISK"
            case "LOW": return "LOW_RISK"
            default: return "UNDEFINED"
        }
    }

    private static boolean EmptyMultiValue(String[] values) {
        return !values || (values.length == 1 && !values[0])
    }

    private String getCompanyNamesByUserId(String userId) {
        def orgs = organizationService.getOrganizationsForUser(userId, DEFAULT_REQUESTER_ID, 0, 100) as List<Organization>
        orgs.sort(true, { a,b -> a.id <=> b.id })
        def result = []
        orgs.each { result += it.abbreviation ?: it.name }
        return result.join(', ')
    }

    private static ReportDataDto packReportTable(ReportTable reportTable)
    {
        ReportDataDto reportDataDto = new ReportDataDto()
        List<ReportTable> reportTables = new ArrayList<ReportTable>()
        reportTables.add(reportTable)
        reportDataDto.setTables(reportTables)
        return reportDataDto
    }

}
