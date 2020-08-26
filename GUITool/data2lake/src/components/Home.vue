<template>
  <div>
    <b-navbar toggleable="lg" type="dark" variant="info">
        <b-navbar-brand href="#">Data2Lake</b-navbar-brand>

        <b-navbar-toggle target="nav-collapse"></b-navbar-toggle>

        <b-collapse id="nav-collapse" is-nav>
          <b-navbar-nav>
            <b-nav-item href="#">Data Connection</b-nav-item>
            <b-nav-item href="#">Tables</b-nav-item>
            <b-nav-item href="#">Subscriptions</b-nav-item>
            <b-nav-item href="#">Others</b-nav-item>
          </b-navbar-nav>
        </b-collapse>
    </b-navbar>
    <b-container fluid="lg">
      <b-form @reset="onReset" @submit.prevent="onSubmit">
        <h2 style=" padding-top: 20px">Database Connection</h2>
        <hr>
        <!-- server name -->
        <b-form-group
          id="input-group-1"
          label="Server Name:"
          label-for="input-1"
          label-class="font-weight-bold"
          :description="config.serverName_comment"
        >
            <b-form-input
                id="input-1"
                v-model="config.serverName"
                placeholder="server name"
            ></b-form-input>
        </b-form-group>
        <!-- port -->
        <b-form-group
          id="input-group-2"
          label="Port:"
          label-for="input-2"
          label-class="font-weight-bold"
          :description="config.port_comment"
        >
            <b-form-input
                id="input-2"
                v-model="config.port"
                placeholder="port"
            ></b-form-input>
        </b-form-group>
        <!-- username -->
        <b-form-group
          id="input-group-3"
          label="Username:"
          label-for="input-3"
          label-class="font-weight-bold"
          :description="config.username_comment"
        >
            <b-form-input
                id="input-3"
                v-model="config.username"
                placeholder="username"
            ></b-form-input>
        </b-form-group>
        <!-- password -->
        <b-form-group
          id="input-group-4"
          label="Password:"
          label-for="input-4"
          label-class="font-weight-bold"
          :description="config.password_comment"
        >
            <b-form-input
                id="input-4"
                v-model="config.password"
                placeholder="password"
            ></b-form-input>
        </b-form-group>
        <!-- egine name -->
        <b-form-group
          id="input-group-5"
          label="Engine Name:"
          label-for="input-5"
          label-class="font-weight-bold"
          :description="config.engineName_comment"
        >
            <b-form-input
                id="input-5"
                v-model="config.engineName"
                placeholder="engineName"
            ></b-form-input>
        </b-form-group>

        <h2>Tables</h2>
        <hr>
        <!-- database name -->
        <b-form-group
          id="input-group-6"
          label="Database Name:"
          label-for="input-6"
          label-class="font-weight-bold"
          :description="config.databaseName_comment"
        >
            <b-form-input
                id="input-6"
                v-model="config.databaseName"
                placeholder="databaseName"
            ></b-form-input>
        </b-form-group>
        <!-- table list -->
        <b-form-group
          id="input-group-12"
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
            <b-button variant="outline-primary" @click="addTableRow" size="sm">Add</b-button>
        </b-form-group>

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

        <h2>Others</h2>
        <hr>
        <!-- VPC -->
        <b-form-group
          id="input-group-7"
          label="VPC:"
          label-class="font-weight-bold"
          label-for="input-7"
          :description="config.vpc_comment"
        >
            <b-form-input
                id="input-7"
                v-model="config.vpc"
                placeholder="vpc"
            ></b-form-input>
        </b-form-group>
        <!-- executive arn -->
        <b-form-group
          id="input-group-8"
          label="Executive Arn:"
          label-for="input-8"
          label-class="font-weight-bold"
          :description="config.executiveArn_comment"
        >
            <b-form-input
                id="input-8"
                v-model="config.executiveArn"
                placeholder="executiveArn"
            ></b-form-input>
        </b-form-group>
        <!-- s3LifecycleRule -->
        <b-form-group
          id="input-group-11"
          label="S3 Lifecycle Rule:"
          label-for="input-11"
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
        <b-button type="reset" variant="danger">Reset</b-button>
        </b-form>
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
    };
  },
  methods: {
    onSubmit() {
      // var content = JSON.stringify(this.config);
      // var blob = new Blob([content], {type: "application/json;charset=utf-8"});
      // saveAs(blob, "config.json");
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
