package reports

import org.openiam.idm.searchbeans.EmailSearchBean
import org.openiam.idm.searchbeans.MetadataTypeSearchBean
import org.openiam.idm.searchbeans.OrganizationSearchBean
import org.openiam.idm.searchbeans.PhoneSearchBean
import org.openiam.idm.searchbeans.RoleSearchBean
import org.openiam.idm.searchbeans.UserSearchBean
import org.openiam.idm.srvc.auth.dto.Login
import org.openiam.idm.srvc.auth.ws.LoginDataWebService
import org.openiam.idm.srvc.continfo.domain.EmailAddressEntity
import org.openiam.idm.srvc.continfo.dto.EmailAddress
import org.openiam.idm.srvc.lang.dto.Language
import org.openiam.idm.srvc.meta.dto.MetadataType
import org.openiam.idm.srvc.meta.ws.MetadataWebService
import org.openiam.idm.srvc.org.service.OrganizationDataService
import org.openiam.idm.srvc.continfo.domain.PhoneEntity
import org.openiam.idm.srvc.org.dto.Organization
import org.openiam.idm.srvc.report.dto.ReportQueryDto
import org.openiam.idm.srvc.report.dto.ReportRow
import org.openiam.idm.srvc.report.dto.ReportRow.ReportColumn

import org.openiam.idm.srvc.report.dto.ReportTable
import org.openiam.idm.srvc.report.service.*
import org.openiam.idm.srvc.report.dto.ReportDataDto
import org.openiam.idm.srvc.role.dto.Role
import org.openiam.idm.srvc.role.ws.RoleDataWebService
import org.openiam.idm.srvc.user.domain.UserEntity
import org.openiam.idm.srvc.user.service.UserDataService
import org.springframework.beans.BeansException
import org.springframework.context.ApplicationContext

import java.text.*

public class NroUserReport implements ReportDataSetBuilder {

    final static String STATUS_META_GROUP = "USER"
    final static String SEC_STATUS_META_GROUP = "USER_2ND_STATUS"
    static final String NRO_OFFICE_TYPE_ID = "replace-with-nro-office-type-id"
	final static String ScriptName = "NroUserReportJG.groovy"

    private ApplicationContext context
    private UserDataService userDataService
    private OrganizationDataService organizationService
    private RoleDataWebService roleDataWebService
    private LoginDataWebService loginDataWebService
    private Language language = getDefaultLanguage()

    DateFormat dateFormat

    @Override
    public ReportDataDto getReportData(ReportQueryDto query) {
		println "=== $ScriptName is called"

        println "=== $ScriptName data source, request: " + query.queryParams.values().join(", ")
        userDataService = context.getBean("userManager")
        loginDataWebService = context.getBean("loginWS")
        metadataWebService = context.getBean("metadataWS")
        organizationService = context.getBean("orgManager")
        roleDataWebService = context.getBean("roleWS")

        def listParameter = query.getParameterValue("GET_VALUES")
        if (listParameter) {
            def table = listValues(listParameter)
            return packReportTable(table)
        }

        def String status = query.getParameterValue("STATUS")
        def String secStatus = query.getParameterValue("SECONDARY_STATUS")
        def String[] roles = query.getParameterValues("ROLE")
        def String[] organizations = query.getParameterValues("ORG_ID")

        def ReportTable reportTable = new ReportTable()

        def isHeadRequest = query.getParameterValue("TABLE") == "HEAD"

        if (isHeadRequest) {

            def ReportRow row = new ReportRow()
            reportTable.setName("head")
            if (status) {
                def bean = getMetadataTypesByGrouping(STATUS_META_GROUP, status)?.get(0)
                if (bean?.displayName) {
                    row.column.add(new ReportColumn('STATUS', bean.displayName))
                }
            }
            if (secStatus) {
                def bean = getMetadataTypesByGrouping(SEC_STATUS_META_GROUP, secStatus)?.get(0)
                if (bean?.displayName) {
                    row.column.add(new ReportColumn('SECONDARY_STATUS', bean.displayName))
                }
            }
            if (!EmptyMultiValue(organizations)) {
                def String names = ""
                for(def String orgId : organizations) {
                    def Organization bean = organizationService.getOrganizationLocalized(orgId, null, language)
                    if (bean?.name) {
                        names += (names ? ", " : "") + bean.name
                    }
                }
                if (names) {
                    row.column.add(new ReportColumn('ORG_ID', names))
                }
            }
            if (!EmptyMultiValue(roles)) {
                def String names = ""
                for(def String roleId : roles) {
                    def Role bean = roleDataWebService.getRole(roleId, null)
                    if (bean?.name) {
                        names += (names ? ", " : "") + bean.name
                    }
                }
                if (names) {
                    row.column.add(new ReportColumn('ROLE', names))
                }
            }
            reportTable.row.add(row)

        } else {

            reportTable.setName("UserReport")

            def messages = validateParameters(organizations, roles, status, secStatus) as String[]
            if (messages) {
                for(def msg : messages) {
                    def ReportRow row = new ReportRow()
                    row.column.add(new ReportColumn('ERROR', msg))
                    reportTable.row.add(row)
                }
            } else {

                UserSearchBean searchBean = new UserSearchBean()
                if (status) {
                    searchBean.setUserStatus(status)
                }
                if (secStatus) {
                    searchBean.setAccountStatus(secStatus)
                }
                if (!EmptyMultiValue(organizations)) {
                    Set<String> orgIds = new HashSet<String>()
                    for(def String orgId : organizations) {
                        orgIds.add(orgId)
                    }
                    searchBean.addOrganizationIdList(orgIds)
                }
                if (!EmptyMultiValue(roles)) {
                    def roleIds = new HashSet<String>() as Set<String>
                    for(def String roleId : roles) {
                        roleIds.add(roleId)
                    }
                    searchBean.setRoleIdSet(roleIds)
                }
                def users = userDataService.getByExample(searchBean, 0, Integer.MAX_VALUE) as List<UserEntity>

                for (UserEntity u : users) {
                    def userId = u.id
                    def ReportRow row = new ReportRow()
                    row.column.add(new ReportColumn('FIRST_NAME', u.firstName))
                    row.column.add(new ReportColumn('MIDDLE_INIT', u.middleInit))
                    row.column.add(new ReportColumn('LAST_NAME', u.lastName))
                    row.column.add(new ReportColumn('TITLE', u.title))
                    row.column.add(new ReportColumn('COMPANY_NAME', getCompanyNamesByUserId(userId)))
                    row.column.add(new ReportColumn('STATUS', u.status?.value))
                    row.column.add(new ReportColumn('EMPLOYEE_ID', u.employeeId))
                    row.column.add(new ReportColumn('EMAIL_ADDRESS', getDefaultEmail(userId)?.replaceAll('@', ' @')))
                    row.column.add(new ReportColumn('PHONE', getDefaultPhone(userId)))
                    Login l = loginDataWebService.getPrimaryIdentity(userId)?.principal
                    row.column.add(new ReportColumn('LAST_LOGIN', l?.lastLogin ? dateFormat.format(l.lastLogin) : null))
                    row.column.add(new ReportColumn('LOGIN', l?.login))

                    reportTable.row.add(row)
                }
            }

        }

        println "=== $ScriptName data source, responce size: " + reportTable.row.size() + " rows"
        return packReportTable(reportTable)
    }

    def validateParameters(String[] organizations, String[] roles, String status, String secStatus) {
        def violations = [] as List
        if (EmptyMultiValue(organizations)) {
            violations.add "Parameter 'NRO office' is required"
        } else {
            if (organizations.find { orgId ->
                organizationService.getOrganizationLocalized(orgId, null, language)?.organizationTypeId != NRO_OFFICE_TYPE_ID
            }) {
                violations.add "Only NRO offices are accepted for parameter 'NRO office'"
            }
        }
        return violations
    }

    def EmptyMultiValue(String[] values) {
        return (!values || values.length == 1 && !values[0])
    }

    private MetadataWebService metadataWebService

    private String getCompanyNamesByUserId(String userId) {
        def orgs = organizationService.getOrganizationsForUser(userId, "3000", 0, 100) as List<Organization>
        orgs.sort(true, { a,b -> a.id <=> b.id })
        def result = []
        orgs.each { result += it.abbreviation ?: it.name }
        return result.join(', ')
    }

    private ReportTable listValues(String parameter) {

        ReportTable reportTable = new ReportTable()
        reportTable.setName("values")

        def ReportRow row

        switch(parameter) {
            case "STATUS":
            case "SECONDARY_STATUS":
                def String metaParameter = parameter == "STATUS" ? STATUS_META_GROUP : SEC_STATUS_META_GROUP
                def List<MetadataType> metadataTypes = getMetadataTypesByGrouping(metaParameter, null)
                for(MetadataType meta : metadataTypes) {
                    row = new ReportRow()
                    row.column.add(new ReportColumn('ID', meta.id))
                    row.column.add(new ReportColumn('NAME', meta.displayName))
                    reportTable.row.add(row)
                }
                break

            case "ROLE":
                def searchBean = new RoleSearchBean(deepCopy: false)
                def List<Role> roles = roleDataWebService.findBeans(searchBean, null, 0, Integer.MAX_VALUE)
                for(Role bean : roles) {
                    row = new ReportRow()
                    row.column.add(new ReportColumn('ID', bean.id))
                    row.column.add(new ReportColumn('NAME', bean.name))
                    reportTable.row.add(row)
                }
                break

        }
        return reportTable
    }

    private List<MetadataType> getMetadataTypesByGrouping(final String grouping, final String id) {
        def MetadataTypeSearchBean searchBean = new MetadataTypeSearchBean()
        searchBean.grouping = grouping
        if (id) {
            searchBean.addKey(id)
        }
        searchBean.active = true
        return metadataWebService.findTypeBeans(searchBean, 0, Integer.MAX_VALUE, language)
    }

    private String getDefaultPhone(String userId) {
        def phones = userDataService.getPhoneList(new PhoneSearchBean(parentId: userId), 100, 0) as List<PhoneEntity>
        def phone = phones ? phones.sort(true, { a,b -> a.isDefault <=> b.isDefault }).get(0) : null
        if (phone) {
            def country = phone.countryCd?.trim() ?: ''
            if (country && !country.startsWith('+')) {
                country = '+' + country
            }
            def area = phone.areaCd ? ('(' + phone.areaCd + ')') : (country ? '-' : '')
            return country + area + (phone.phoneNbr ?: '') + (phone.phoneExt ? ' +' + phone.phoneExt : '')
        }
        return ''
    }

    private String getDefaultEmail(String userId) {
        def emails = userDataService.getEmailAddressList(new EmailSearchBean(parentId: userId), 100, 0) as List<EmailAddressEntity>
        return emails ? emails.sort(true, { a,b -> a.isDefault <=> b.isDefault }).get(0).emailAddress : ''
    }

    private static ReportDataDto packReportTable(ReportTable reportTable)
    {
        ReportDataDto reportDataDto = new ReportDataDto()
        List<ReportTable> reportTables = new ArrayList<ReportTable>()
        reportTables.add(reportTable)
        reportDataDto.setTables(reportTables)
        return reportDataDto
    }

    private static Language getDefaultLanguage(){
        return new Language(id:  1)
    }

    @Override
    void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext
    }
}
