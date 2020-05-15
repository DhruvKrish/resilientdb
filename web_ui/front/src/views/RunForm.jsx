/*!

=========================================================
* Black Dashboard React v1.0.0
=========================================================

* Product Page: https://www.creative-tim.com/product/black-dashboard-react
* Copyright 2019 Creative Tim (https://www.creative-tim.com)
* Licensed under MIT (https://github.com/creativetimofficial/black-dashboard-react/blob/master/LICENSE.md)

* Coded by Creative Tim

=========================================================

* The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

*/
import React from "react";

// reactstrap components
import {
    Button,
    Card,
    CardHeader,
    CardBody,
    CardFooter,
    CardText,
    CardTitle,
    Table,
    FormGroup,
    Form,
    Input,
    Row,
    Col
} from "reactstrap";

class RunForm extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            nodes: 4,
            clients: 1,
            max_inf: 20000,
            log: [],
            running: 0
        };

        this.handleChange = this.handleChange.bind(this);
        this.handleSubmit = this.handleSubmit.bind(this);
    }

    async componentDidMount() {
        try {
            setInterval(async () => {
                const res = await fetch('/api/status');
                const result = await res.json();
                this.setState({
                    log: result.status,
                })
            }, 2000);
        } catch (e) {
            console.log(e);
        }
    }

    renderLog() {
        return this.state.log.map((log_text, index) =>
            <tr key={index}>
                <td style={{ width: '50px' }}>{index}</td>
                <td>{log_text}</td>
            </tr>)
    }

    handleChange(event) {
        this.setState({ [event.target.name]: event.target.value }, () => console.log(this.state));
    }

    handleSubmit(event) {
        const data = {
            nodes: this.state.nodes,
            clients: this.state.clients,
            max_inf: this.state.max_inf
        }
        const param = Object.keys(data).map(key => key + '=' + data[key]).join('&');
        fetch('http:/api/run?' + param).then((response) => {
            response.json().then(res => {
                console.log(res)
                if (res.result == 200) {
                    this.setState({ running: 1 });
                }
                alert(res.status)
            })

        });

    }
    render() {
        return (
            <>
                <div className="content">
                    <Row>
                        <Col md="12">
                            <Card>
                                <CardHeader>
                                    <h5 className="title">Run Parameters</h5>
                                </CardHeader>
                                <CardBody>
                                    <Form>
                                        <Row>
                                            <Col className="pr-md-1" md="6">
                                                <FormGroup>
                                                    <label>Number of nodes(replicas):</label>
                                                    <Input
                                                        onChange={this.handleChange}
                                                        name="nodes"
                                                        defaultValue="4"
                                                        placeholder="4"
                                                        type="number"
                                                    />
                                                </FormGroup>
                                            </Col>
                                            <Col className="px-md-1" md="6">
                                                <FormGroup>
                                                    <label>Number of Clients</label>
                                                    <Input
                                                        onChange={this.handleChange}
                                                        defaultValue="1"
                                                        name="clients"
                                                        placeholder="1"
                                                        type="number"
                                                    />
                                                </FormGroup>
                                            </Col>
                                        </Row>
                                        <Row>
                                            <Col className="pr-md-1" md="6">
                                                <FormGroup>
                                                    <label>Maximum Transaction Inflight per Client Name</label>
                                                    <Input
                                                        onChange={this.handleChange}
                                                        name="max_inf"
                                                        defaultValue="20000"
                                                        placeholder="20000"
                                                        type="number"
                                                    />
                                                </FormGroup>
                                            </Col>
                                        </Row>
                                    </Form>
                                </CardBody>
                                <CardFooter>
                                    {this.state.running ? "" : <Button className="btn-fill" color="primary" type="submit" onClick={this.handleSubmit}>
                                        Run
                                    </Button>}
                                    
                                    <Button className="btn-fill" color="secondry" onClick={()=> window.open("/chrono", "_blank")}>    
                                            Stats
                                    </Button>
                                    

                                </CardFooter>
                            </Card>
                        </Col>
                    </Row>
                    <Row>
                        <Col md="12">
                            <Card>
                                <CardHeader>
                                    <CardTitle tag="h4">Run Log</CardTitle>
                                </CardHeader>
                                <CardBody>
                                    <Table className="tablesorter" responsive>
                                        <thead className="text-primary">
                                            <tr>
                                                <th>#</th>
                                                <th>Text</th>

                                            </tr>
                                        </thead>
                                        <tbody>
                                            {this.renderLog()}
                                        </tbody>
                                    </Table>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                </div>
            </>
        );
    }
}

export default RunForm;
