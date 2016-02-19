<table>


<#list resultRowList as resultRow>
   <tr>
        <#list resultRow as col>
            <td>${col}</td>
        </#list>
   </tr>
</#list>

</table>