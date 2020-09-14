<template>
  <div>
    <b-navbar toggleable="lg" type="dark" variant="info" sticky="true">
        <b-navbar-brand href="#top">Data2Lake</b-navbar-brand>

        <b-navbar-toggle target="nav-collapse"></b-navbar-toggle>

        <b-collapse id="nav-collapse" is-nav>
          <b-navbar-nav>
            <b-nav-item href="#databaseConnection">Data Connection</b-nav-item>
            <b-nav-item href="#tables">Tables</b-nav-item>
            <b-nav-item href="#subscriptions">Subscriptions</b-nav-item>
            <b-nav-item href="#others">Others</b-nav-item>
          </b-navbar-nav>
        </b-collapse>
    </b-navbar>
    <b-container fluid="lg">
      <section id="databaseConnection"></section>
      <b-form @reset="onReset" @submit.prevent="onSubmit">
        <h2 style=" padding-top: 20px">Database Connection</h2>
        <hr>
        <!-- server name -->
        <b-form-group
          label="Server Name:"
          label-class="font-weight-bold"
          :description="config.serverName_comment"
        >
            <b-form-input
                v-model="config.serverName"
                placeholder="server name"
            ></b-form-input>
        </b-form-group>
        <!-- port -->
        <b-form-group
          label="Port:"
          label-class="font-weight-bold"
          :description="config.port_comment"
        >
            <b-form-input
                v-model.number="config.port"
                placeholder="port"
                type="number"
            ></b-form-input>
        </b-form-group>
        <!-- username -->
        <b-form-group
          label="Username:"
          label-class="font-weight-bold"
          :description="config.username_comment"
        >
            <b-form-input
                v-model="config.username"
                placeholder="username"
            ></b-form-input>
        </b-form-group>
        <!-- password -->
        <b-form-group
          label="Password:"
          label-class="font-weight-bold"
          :description="config.password_comment"
        >
            <b-form-input
                v-model="config.password"
                placeholder="password"
            ></b-form-input>
        </b-form-group>
        <!-- egine name -->
        <b-form-group
          label="Engine Name:"
          label-class="font-weight-bold"
          :description="config.engineName_comment"
        >
            <b-form-select v-model="config.engineName" :options="options"></b-form-select>
        </b-form-group>
        <!-- database name -->
        <b-form-group
          label="Database Name:"
          label-class="font-weight-bold"
          :description="config.databaseName_comment"
        >
            <b-form-input
                v-model="config.databaseName"
                placeholder="databaseName"
            ></b-form-input>
        </b-form-group>

        <section id="tables"></section>
        <h2>Tables</h2>
        <hr>
        <!-- table list -->
        <b-form-group
          label="Table List:"
          :description="config.tableList_comment"
          label-class="font-weight-bold"
        >
            <div 
                v-for="(table, index) in config.tableList"
                :key="index">
                <b-badge>source table {{index+1}}</b-badge>
                <b-form-group label-cols="4" label-cols-lg="2" label-size="sm" label="Schema Name">
                    <b-form-input size="sm" v-model="config.tableList[index].schemaName"></b-form-input>
                </b-form-group>
                <b-form-group label-cols="4" label-cols-lg="2" label-size="sm" label="Table Name">
                    <b-form-input size="sm" v-model="config.tableList[index].tableName"></b-form-input>
                </b-form-group>
                <b-button variant="danger" @click="deleteTableRow(index)" size="sm">Remove</b-button>
                <hr>
            </div>
            <b-button variant="outline-primary" size="sm" @click="addTableRow">Add</b-button>
            <b-button variant="outline-info" v-b-modal.tableSelect size="sm" style="margin-left:5px">DB Connector</b-button>
        </b-form-group>

        <section id="subscriptions"></section>
        <h2>Subscription</h2>
        <hr>
        <!-- email subscription -->
        <b-form-group
            label="Email Subscriptions:"
            label-class="font-weight-bold"
            :description="config.emailSubscriptionList_comment">
            <div
                v-for="(email, index) in config.emailSubscriptionList"
                :key="index"
                style="margin-left: 60px;">
                <b-badge>email {{index+1}}</b-badge>
                <b-form-group label-cols="4" label-cols-lg="2" label-size="sm" label="Email">
                    <b-form-input
                        size="sm"
                        v-model="config.emailSubscriptionList[index]"
                        style="margin-bottom: 5px;"
                        type="email"
                    ></b-form-input>
                </b-form-group>
                <b-button variant="danger" @click="deleteEmailRow(index)" size="sm">Remove</b-button>
                <hr>
            </div>
            <b-button variant="outline-primary" @click="addEmailRow" size="sm">Add</b-button>
        </b-form-group>
        <!-- sms subscription -->
        <b-form-group
            label="SMS Subscriptions:"
            label-class="font-weight-bold"
            :description="config.smsSubscriptionList_comment">
            <div
                v-for="(sms, index) in config.smsSubscriptionList"
                :key="index"
                style="margin-left: 60px;">
                <b-badge>phone {{index+1}}</b-badge>
                <b-form-group label-cols="4" label-cols-lg="2" label-size="sm" label="Phone Number" >
                    <b-form-input
                        size="sm"
                        v-model="config.smsSubscriptionList[index]"
                        style="margin-bottom: 5px;"
                    ></b-form-input>
                </b-form-group>
                <b-button variant="danger" @click="deleteSMSRow(index)" size="sm">Remove</b-button>
                <hr>
            </div>
            <b-button variant="outline-primary" @click="addSMSRow" size="sm">Add</b-button>
        </b-form-group>

        <section id="others"></section>
        <h2>Others</h2>
        <hr>
        <!-- VPC -->
        <b-form-group
          label="VPC:"
          label-class="font-weight-bold"
          :description="config.vpc_comment"
        >
            <b-form-input
                v-model="config.vpc"
                placeholder="vpc"
            ></b-form-input>
        </b-form-group>
        <!-- executive arn -->
        <b-form-group
          label="Executive Arn:"
          label-class="font-weight-bold"
          :description="config.executiveArn_comment"
        >
            <b-form-input
                v-model="config.executiveArn"
                placeholder="executiveArn"
            ></b-form-input>
        </b-form-group>
        <!-- s3LifecycleRule -->
        <b-form-group
          label="S3 Lifecycle Rule:"
          label-class="font-weight-bold"
          :description="config.s3LifecycleRule_comment"
        >
            <div 
                v-for="(rule, index) in config.s3LifecycleRule"
                :key="index">
                <b-badge>rule {{index+1}}</b-badge>
                <b-form-checkbox
                        v-model="config.s3LifecycleRule[index].enabled"
                        switch
                >
                enabled
                </b-form-checkbox>
                <b-form-group label-cols="5" label-cols-lg="3" label-size="sm" label="Expiration">
                    <b-form-input size="sm" v-model="config.s3LifecycleRule[index].expiration"></b-form-input>
                </b-form-group>
                <b-form-group label-cols="5" label-cols-lg="3" label-size="sm" label="Prefix">
                    <b-form-input size="sm" v-model="config.s3LifecycleRule[index].prefix"></b-form-input>
                </b-form-group>
                <b-form-group label-cols="5" label-cols-lg="3" label-size="sm" label="AbortIncompleteMultipartUploadAfter">
                    <b-form-input size="sm" v-model="config.s3LifecycleRule[index].abortIncompleteMultipartUploadAfter"></b-form-input>
                </b-form-group>
                <hr>
            </div>
            <b-button variant="outline-primary" @click="adds3LifecycleRuleRow" size="sm">Add</b-button>
        </b-form-group>

        <b-button variant="primary" type="submit">Update Configuration</b-button>
        <b-button type="reset" variant="danger" style="margin-left:5px">Reset</b-button>
        </b-form>
        <b-modal id="tableSelect" title="Tables" @ok="doneRowSelected">
          <b-button @click="connectDB" size="sm">Connect to Database</b-button>
          <b-table
            selectable
            :select-mode="multi"
            :items="tables"
            :fields="fields"
            @row-selected="onRowSelected"
            sticky-header="true"
            max-height="500px"
            style="margin-top:5px"
          >
            <template v-slot:cell(selected)="{ rowSelected }">
              <template v-if="rowSelected">
                <span aria-hidden="true">&check;</span>
                <span class="sr-only">Selected</span>
              </template>
              <template v-else>
                <span aria-hidden="true">&nbsp;</span>
                <span class="sr-only">Not selected</span>
              </template>
            </template>
          </b-table>
        </b-modal>
    </b-container>
  </div>
</template>

<script>
import axios from 'axios'

export default {
  name: 'Home',
  props: {

  },
  data() {
    return {
      config: {},
      options: [
        { value: 'mysql', text: 'mysql' },
        { value: 'postgres', text: 'postgres' }
      ],
      connection: null,
      tables: [],
      selected: [],
      fields: ['selected', 'schema', 'table'],
    };
  },
  methods: {
    onSubmit() {
      var content = JSON.stringify(this.config);
      let uri = 'data:text/csv;charset=utf-8,\ufeff' + encodeURIComponent(content);
      let link = document.createElement("a");
      link.href = uri;
      link.download =  "config.json";
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
    },
    onReset() {
      this.config.emailSubscriptionList = [];
      this.config.smsSubscriptionList = [];
      this.config.s3LifecycleRule = [];
      this.config.tableList = [];
    },
    addEmailRow() {
      this.config.emailSubscriptionList.push("");
    },
    deleteEmailRow(index) {
      this.config.emailSubscriptionList.splice(index, 1);
    },
    addSMSRow() {
      this.config.smsSubscriptionList.push("");
    },
    deleteSMSRow(index) {
      this.config.smsSubscriptionList.splice(index, 1);
    },
    adds3LifecycleRuleRow() {
      this.config.s3LifecycleRule.push({
        enabled: false,
        expiration: 10,
        prefix: "prefix_",
        abortIncompleteMultipartUploadAfter: 3,
      });
    },
    deleteS3LifecycleRuleRow(index) {
      this.config.s3LifecycleRule.splice(index, 1);
    },
    addTableRow() {
      this.config.tableList.push({
        schemaName: "",
        tableName: "",
      });
    },
    deleteTableRow(index) {
      this.config.tableList.splice(index, 1);
    },
    connectDB() {
      let url = 'http://localhost:7788/tables/'+this.config.engineName+'/'
        +this.config.serverName+'/'+this.config.port+'/'+this.config.username+'/'
        +this.config.password+'/'+this.config.databaseName;
      axios.get(url)
      .then( res => {
        this.tables = res.data;
      })
      .catch( err => {
        console.log(err)
      })
    },
    onRowSelected(items) {
      this.selected = items;
    },
    doneRowSelected() {
      console.log(this.selected, this.tables)
      if (this.selected.length == this.tables.length && this.tables.length != 0) {
        this.config.tableList = [{ "schemaName": this.tables[0].schema, "tableName": "%"}]
        return;
      }
      this.config.tableList = this.selected.map( item => {
        return { "schemaName": item.schema, "tableName": item.table }
      })
    }
  },
  mounted() { 
    axios.get('config.json')
    .then( res => {
      this.config = res.data
    })
  },
};
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
body {
  background-color: rgb(240, 240, 240);
}
.navbar {
  padding-top: 10px;
}
</style>
