import { Dialog, showDialog } from '@jupyterlab/apputils';

import { IClusterFactoryModel } from './clusters';
import * as React from 'react';

/**
 * A namespace for ClusterCreating statics.
 */
namespace ClusterCreating {
  /**
   * The props for the ClusterCreating component.
   */

  export interface IProps {
    /**
     * The initial cluster model shown in the scaling.
     */
    initialModel: IClusterFactoryModel[];

    /**
     * A callback that allows the component to write state to an
     * external object.
     */
    stateEscapeHatch: (model: IClusterFactoryModel[]) => void;
  }

  /**
   * The state for the ClusterCreating component.
   */
  export interface IState {
    /**
     * The proposed cluster model shown in the scaling.
     */
    model: IClusterFactoryModel[];
  }
}

/**
 * A component for an HTML form that allows the user
 * to select scaling parameters.
 */
export class ClusterCreating extends React.Component<
  ClusterCreating.IProps,
  ClusterCreating.IState
> {

  /**
   * Construct a new ClusterCreating component.
   */
  constructor(props: ClusterCreating.IProps) {
    super(props);
    let model: IClusterFactoryModel[];
    // If the initial model is static, enrich it
    // with placeholder values for minimum and maximum workers.
    model = props.initialModel;

    this.state = { model };
  }

  /**
   * When the component updates we take the opportunity to write
   * the state of the cluster to an external object so this can
   * be sent as the result of the dialog.
   */
  componentDidUpdate(): void {
    let model: IClusterFactoryModel[] = { ...this.state.model };
    this.props.stateEscapeHatch(model);
  }

  onFactoryChange(event: React.ChangeEvent): void {
    console.log("onFactoryChange", event);

    let currentSelection = (event.target as HTMLInputElement).value;
    console.log("onFactoryChange", currentSelection);

    console.log("onFactoryChange", this.state)

    this.state.model.forEach((val: IClusterFactoryModel) => {
      if (val.name === currentSelection) {
        val.selected = true;
      } else {
        val.selected = false;
      }
    })

    this.setState({
      model: this.state.model,
    });
  }

  onImageChange(event: React.ChangeEvent): void {
    console.log("onFactoryChange", event);

    let currentSelection = (event.target as HTMLInputElement).value;
    console.log("onFactoryChange", currentSelection);

    console.log("onFactoryChange", this.state)

    this.state.model.forEach((val: IClusterFactoryModel) => {
        val.singularityImage = currentSelection
    })

    this.setState({
      model: this.state.model,
    });
  }

  onCoresChange(event: React.ChangeEvent): void {
    let currentCores = parseInt((event.target as HTMLInputElement).value, 10);
    this.state.model.forEach((val: IClusterFactoryModel) => {
      val.user_cores = currentCores;
    })
    this.setState({ model: this.state.model });
  }

  onMemoryChange(event: React.ChangeEvent): void { 
    let currentSelection = (event.target as HTMLInputElement).value;
    this.state.model.forEach((val: IClusterFactoryModel) => {
      val.user_memory = currentSelection;
    });
    this.setState({ model: this.state.model });
  }

  /**
   * Render the component..
   */
  render() {
    const model = this.state.model;
    console.log("render", model);
    let options = model.map((data) =>
      <option
        key={data.name}
        value={data.name}
      >
        {data.name}
      </option>
    );
    return (
      <div>
        <span className="dask-ScalingHeader">Factory</span>
        <div className="dask-ScalingSection">
          <div className="dask-ScalingSection-item">
            Name:
            <select name="customSearch" className="custom-search-select" onChange={evt => {
              this.onFactoryChange(evt);
            }}>
              <option>Select Item</option>
              {options}
            </select>
          </div>
          <div className="dask-ScalingSection-item">
              WN Image:
              <input list="ice-cream-flavors" id="ice-cream-choice" name="ice-cream-choice" onChange={evt => {
              this.onImageChange(evt);
            }}/>
            <datalist id="ice-cream-flavors">
              <option value="/cvmfs/unpacked.cern.ch/ghcr.io/comp-dev-cms-ita/kernel-root:v0.0.11-rc3"> Base ROOT 6.32.00 + CMSJMECalculator main@1341dce26396def7860929af944d0612540fa0a5 + Correctionlib 2.6.4 </option>
              <option value="/cvmfs/unpacked.cern.ch/ghcr.io/comp-dev-cms-ita/kernel-coffea:v0.0.11-rc3"> Base Coffea 0.7.22 </option>
              <option value="/cvmfs/unpacked.cern.ch/ghcr.io/comp-dev-cms-ita/kernel-mkshapesrdf:v0.0.11-rc3"> mkShapesRDF master@2f3a5427e4dc0af4d54afb55219207e0d2b51bce </option>
              <option value="/cvmfs/unpacked.cern.ch/ghcr.io/comp-dev-cms-ita/kernel-pocketcoffea:v0.0.11-rc3"> PocketCoffea 0.9.8  </option>
            </datalist>
          </div>
          <div className="dask-ScalingSection-item">
            Cores per HTCondor job:
            <input type="number" min="1" max="4" onChange={evt => {
              this.onCoresChange(evt);
            }} />
          </div>
          <div className="dask-ScalingSection-item"> 
            Memory per HTCondor job:
            <input list="ice-cream-flavors-2" id="ice-cream-choice-2" name="ice-cream-choice-2" onChange={evt => {
              this.onMemoryChange(evt);
            }}/>
            <datalist id="ice-cream-flavors-2">
              <option value="2 GiB"> 2 GiB </option>
              <option value="4 GiB"> 4 GiB </option>
            </datalist>
          </div>
        </div>
      </div>
    );
  }
}

/**
 * Show a dialog for scaling a cluster model.
 *
 * @param model: the initial model.
 *
 * @returns a promse that resolves with the user-selected scalings for the
 *   cluster model. If they pressed the cancel button, it resolves with
 *   the original model.
 */
export async function showCreatingDialog(
  factories: IClusterFactoryModel[]
): Promise<IClusterFactoryModel> {
  let newModel = { ...factories };
  const escapeHatch = (update: IClusterFactoryModel[]) => {
    newModel = update;
  };

  return showDialog({
    title: "Create new cluster",
    body: (
      <ClusterCreating initialModel={factories} stateEscapeHatch={escapeHatch} />
    ),
    buttons: [Dialog.cancelButton(), Dialog.okButton({ label: 'CREATE' })]
  }).then(result => {
    console.log("showCreatingDialog", result)
    console.log("showCreatingDialog", factories)
    console.log("showCreatingDialog", newModel)
    if (result.button.accept) {
      for (let index in newModel) {
        if (newModel.hasOwnProperty(index)) {
          let curFactory = newModel[index];
          if (curFactory.selected === true) {
            return curFactory;
          }
        }
      }
      return { "name": "undefined", "selected": false, "singularityImage": "DUMMY", user_cores: 1, user_memory: "DUMMY"};
    } else {
      return { "name": "undefined", "selected": false, "singularityImage": "DUMMY", user_cores: 1, user_memory: "DUMMY"};
    }
  });
}
